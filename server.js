import express from "express";
import { createServer } from "http";
import { WebSocketServer } from "ws";
import { randomUUID } from "crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { z } from "zod";

const TOKEN = process.env.TOKEN;
if (!TOKEN) {
  console.error("Umgebungsvariable TOKEN fehlt");
  process.exit(1);
}

const app = express();
app.use(express.json({ limit: "5mb" }));

const httpServer = createServer(app);
const wss = new WebSocketServer({ noServer: true });

let client = null;
const pending = new Map();

httpServer.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname !== "/ws" || url.searchParams.get("token") !== TOKEN) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws));
});

wss.on("connection", (ws) => {
  if (client && client.readyState === 1) client.close();
  client = ws;
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const entry = pending.get(msg.id);
    if (!entry) return;
    pending.delete(msg.id);
    clearTimeout(entry.timer);
    entry.resolve(msg);
  });
  ws.on("close", () => {
    if (client === ws) client = null;
  });
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

function isConnected() {
  return client !== null && client.readyState === 1;
}

function runScript(code, timeoutMs) {
  return new Promise((resolve, reject) => {
    if (!isConnected()) return reject(new Error("Delta ist nicht verbunden"));
    const id = randomUUID();
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error("Zeitüberschreitung: keine Antwort von Delta"));
    }, timeoutMs);
    pending.set(id, { resolve, timer });
    client.send(JSON.stringify({ id, code }));
  });
}

function buildMcpServer() {
  const server = new McpServer({ name: "delta", version: "1.0.0" });

  server.tool(
    "execute_script",
    "Führt Luau-Code im verbundenen Delta-Executor aus und gibt print/warn-Ausgaben, den Rückgabewert und Fehler zurück.",
    {
      code: z.string().describe("Luau-Code, der ausgeführt werden soll"),
      timeout_seconds: z.number().min(1).max(60).optional(),
    },
    async ({ code, timeout_seconds }) => {
      try {
        const r = await runScript(code, (timeout_seconds ?? 15) * 1000);
        const parts = [r.ok ? "OK" : "FEHLER"];
        if (r.output) parts.push("Ausgabe:\n" + r.output);
        if (r.result) parts.push("Rückgabe:\n" + r.result);
        if (r.error) parts.push("Fehler:\n" + r.error);
        return {
          content: [{ type: "text", text: parts.join("\n\n") }],
          isError: !r.ok,
        };
      } catch (e) {
        return { content: [{ type: "text", text: e.message }], isError: true };
      }
    }
  );

  server.tool(
    "status",
    "Zeigt, ob Delta aktuell mit dem Server verbunden ist.",
    {},
    async () => ({
      content: [
        { type: "text", text: isConnected() ? "verbunden" : "nicht verbunden" },
      ],
    })
  );

  return server;
}

app.post("/mcp/:token", async (req, res) => {
  if (req.params.token !== TOKEN) {
    res.status(401).json({ error: "Nicht erlaubt" });
    return;
  }
  const server = buildMcpServer();
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
  });
  res.on("close", () => {
    transport.close();
    server.close();
  });
  await server.connect(transport);
  await transport.handleRequest(req, res, req.body);
});

app.get("/mcp/:token", (req, res) => res.status(405).end());
app.delete("/mcp/:token", (req, res) => res.status(405).end());
app.get("/", (req, res) => res.send("ok"));

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => console.log("läuft auf Port " + PORT));
