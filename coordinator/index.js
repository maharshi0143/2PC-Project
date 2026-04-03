const express = require("express");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const http = require("http");
const cors = require("cors");
const WebSocket = require("ws");
const { randomUUID } = require("crypto");

const app = express();
const allowedOrigins = (process.env.CORS_ORIGINS || "http://localhost:8081,http://localhost:3000")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

app.use(
  cors({
    origin(origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
        return;
      }

      callback(new Error("Not allowed by CORS"));
    },
    methods: ["GET", "POST"],
  })
);
app.use(express.json());

const WAL_FILE = process.env.WAL_FILE || "./data/coordinator.log";
const PARTICIPANTS = [
  { service: "inventory", baseUrl: "http://inventory:3001" },
  { service: "orders", baseUrl: "http://orders:3002" },
];
const PARTICIPANT_REQUEST_TIMEOUT_MS = Number(process.env.PARTICIPANT_REQUEST_TIMEOUT_MS || 5000);
const CHAOS_SERVICE_MAP = {
  inventory: "inventory",
  orders: "orders",
  "inventory-service": "inventory",
  "orders-service": "orders",
};
const CHAOS_ALLOWLIST = new Set(Object.keys(CHAOS_SERVICE_MAP));
let pauseAfterDecisionMs = 0;

// In-memory transaction state
const transactions = {};

// Store WebSocket clients
let clients = [];

/**
 * WAL Logger
 */
function ensureWalFile() {
  fs.mkdirSync(path.dirname(WAL_FILE), { recursive: true });
  if (!fs.existsSync(WAL_FILE)) {
    fs.writeFileSync(WAL_FILE, "", "utf8");
  }
}

function writeLog(entry) {
  ensureWalFile();
  fs.appendFileSync(
    WAL_FILE,
    JSON.stringify({
      timestamp: new Date().toISOString(),
      ...entry,
    }) + "\n"
  );
}

/**
 * Update transaction state
 */
function updateState(txnId, status) {
  transactions[txnId] = status;
}

function validateItems(rawItems) {
  if (!Array.isArray(rawItems) || rawItems.length === 0) {
    return { valid: false, message: "items must be a non-empty array" };
  }

  if (rawItems.length > 100) {
    return { valid: false, message: "items cannot contain more than 100 entries" };
  }

  const normalized = [];
  for (const item of rawItems) {
    const sku = String(item?.sku || "")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9_-]/g, "")
      .slice(0, 64);

    const quantity = Number(item?.quantity);
    if (!sku) {
      return { valid: false, message: "each item must contain a valid sku" };
    }

    if (!Number.isInteger(quantity) || quantity <= 0 || quantity > 100000) {
      return { valid: false, message: `invalid quantity for sku ${sku}` };
    }

    normalized.push({ sku, quantity });
  }

  return { valid: true, items: normalized };
}

async function sendPhase2(action, txnId) {
  const endpoint = action === "commit" ? "/commit" : "/rollback";
  const payload = { transaction_id: txnId };

  const results = await Promise.allSettled(
    PARTICIPANTS.map((participant) =>
      axios.post(`${participant.baseUrl}${endpoint}`, payload, {
        timeout: PARTICIPANT_REQUEST_TIMEOUT_MS,
      })
    )
  );

  return results;
}

function hasPhase2Failures(results) {
  return results.some((result) => result.status === "rejected");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseWalByTransaction() {
  ensureWalFile();
  const lines = fs
    .readFileSync(WAL_FILE, "utf8")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const byTxn = new Map();

  for (const line of lines) {
    try {
      const entry = JSON.parse(line);
      const transactionId = entry.transaction_id || entry.txnId;
      if (!transactionId || !entry.state) {
        continue;
      }

      const current = byTxn.get(transactionId) || {
        transaction_id: transactionId,
        state: "UNKNOWN",
        decision: null,
        ended: false,
      };

      current.state = entry.state;
      if (entry.state === "GLOBAL_COMMIT") {
        current.decision = "GLOBAL_COMMIT";
      }

      if (entry.state === "GLOBAL_ABORT") {
        current.decision = "GLOBAL_ABORT";
      }

      if (entry.state === "END") {
        current.ended = true;
      }

      byTxn.set(transactionId, current);
    } catch (error) {
      console.error("Skipping invalid WAL line:", error.message);
    }
  }

  return byTxn;
}

function resolveStatusFromWal(txnId) {
  const walEntry = parseWalByTransaction().get(txnId);
  if (!walEntry) {
    return "UNKNOWN";
  }

  if (walEntry.ended) {
    if (walEntry.decision === "GLOBAL_COMMIT") {
      return "COMMITTED";
    }

    if (walEntry.decision === "GLOBAL_ABORT") {
      return "ABORTED";
    }
  }

  if (walEntry.decision === "GLOBAL_COMMIT") {
    return "COMMITTING";
  }

  if (walEntry.decision === "GLOBAL_ABORT") {
    return "ABORTING";
  }

  if (walEntry.state === "BEGIN") {
    return "PREPARING";
  }

  return "UNKNOWN";
}

async function recoverFromWal() {
  const pending = [...parseWalByTransaction().values()].filter((entry) => !entry.ended);
  if (pending.length === 0) {
    return;
  }

  console.log(`WAL recovery found ${pending.length} in-flight transaction(s)`);

  for (const entry of pending) {
    const txnId = entry.transaction_id;

    if (entry.state === "BEGIN") {
      writeLog({ transaction_id: txnId, state: "GLOBAL_ABORT" });
      updateState(txnId, "ABORTING");
      broadcastUpdate(txnId, "ABORTING");

      await sendPhase2("rollback", txnId);

      updateState(txnId, "ABORTED");
      broadcastUpdate(txnId, "ABORTED");
      writeLog({ transaction_id: txnId, state: "END" });
      continue;
    }

    if (entry.decision === "GLOBAL_COMMIT" || entry.state === "GLOBAL_COMMIT") {
      updateState(txnId, "COMMITTING");
      broadcastUpdate(txnId, "COMMITTING");

      await sendPhase2("commit", txnId);

      updateState(txnId, "COMMITTED");
      broadcastUpdate(txnId, "COMMITTED");
      writeLog({ transaction_id: txnId, state: "END" });
      continue;
    }

    if (entry.decision === "GLOBAL_ABORT" || entry.state === "GLOBAL_ABORT") {
      updateState(txnId, "ABORTING");
      broadcastUpdate(txnId, "ABORTING");

      await sendPhase2("rollback", txnId);

      updateState(txnId, "ABORTED");
      broadcastUpdate(txnId, "ABORTED");
      writeLog({ transaction_id: txnId, state: "END" });
    }
  }
}

function dockerRequest(method, requestPath) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        socketPath: "/var/run/docker.sock",
        path: requestPath,
        method,
        headers: {
          "Content-Type": "application/json",
        },
      },
      (res) => {
        let body = "";
        res.on("data", (chunk) => {
          body += chunk;
        });
        res.on("end", () => {
          resolve({ statusCode: res.statusCode || 500, body });
        });
      }
    );

    req.on("error", reject);
    req.end();
  });
}

async function stopComposeService(serviceName) {
  const filters = encodeURIComponent(
    JSON.stringify({
      label: [`com.docker.compose.service=${serviceName}`],
    })
  );

  const list = await dockerRequest("GET", `/containers/json?all=1&filters=${filters}`);
  if (list.statusCode < 200 || list.statusCode >= 300) {
    throw new Error("Could not query Docker containers");
  }

  const containers = JSON.parse(list.body || "[]");
  if (containers.length === 0) {
    throw new Error(`No container found for service ${serviceName}`);
  }

  const containerId = containers[0].Id;
  const containerState = String(containers[0].State || "").toLowerCase();

  if (containerState !== "running") {
    return {
      containerId,
      alreadyStopped: true,
    };
  }

  const stop = await dockerRequest("POST", `/containers/${containerId}/stop?t=2`);

  if (stop.statusCode < 200 || stop.statusCode >= 300) {
    throw new Error(`Failed to stop ${serviceName}`);
  }

  return {
    containerId,
    alreadyStopped: false,
  };
}

/**
 * Broadcast to all WebSocket clients
 */
function broadcastUpdate(txnId, status) {
  const message = JSON.stringify({
    type: "TRANSACTION_UPDATE",
    payload: {
      transaction_id: txnId,
      status,
      timestamp: new Date().toISOString(),
    },
  });

  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

/**
 * POST /api/transactions
 */
app.post("/api/transactions", async (req, res) => {
  const validation = validateItems(req.body?.items);
  if (!validation.valid) {
    return res.status(400).json({ error: validation.message });
  }

  const txnId = randomUUID();
  const items = validation.items;

  console.log("NEW TRANSACTION:", txnId);

  try {
    // STEP 1: BEGIN
    writeLog({ transaction_id: txnId, state: "BEGIN", payload: { items } });

    updateState(txnId, "PREPARING");
    broadcastUpdate(txnId, "PREPARING");

    // STEP 2: PREPARE
    const prepareInventory = axios.post("http://inventory:3001/prepare", {
      transaction_id: txnId,
      items,
    }, {
      timeout: PARTICIPANT_REQUEST_TIMEOUT_MS,
    });

    const prepareOrders = axios.post("http://orders:3002/prepare", {
      transaction_id: txnId,
      items,
    }, {
      timeout: PARTICIPANT_REQUEST_TIMEOUT_MS,
    });

    const results = await Promise.allSettled([
      prepareInventory,
      prepareOrders,
    ]);

    const allSuccess = results.every(
      (r) => r.status === "fulfilled"
    );

    // STEP 3: DECISION
    if (allSuccess) {
      writeLog({ transaction_id: txnId, state: "GLOBAL_COMMIT" });

      updateState(txnId, "COMMITTING");
      broadcastUpdate(txnId, "COMMITTING");

      if (pauseAfterDecisionMs > 0) {
        await sleep(pauseAfterDecisionMs);
      }

      // STEP 4: COMMIT
      const commitResults = await sendPhase2("commit", txnId);
      if (hasPhase2Failures(commitResults)) {
        console.error(`Commit acknowledgements incomplete for ${txnId}. Recovery will retry.`);
        return res.status(500).json({
          transaction_id: txnId,
          status: "COMMITTING",
          reason: "Commit acknowledgements incomplete",
        });
      }

      updateState(txnId, "COMMITTED");
      broadcastUpdate(txnId, "COMMITTED");

      writeLog({ transaction_id: txnId, state: "END" });

      return res.status(201).json({
        transaction_id: txnId,
        status: "COMMITTED",
      });
    } else {
      writeLog({ transaction_id: txnId, state: "GLOBAL_ABORT" });

      updateState(txnId, "ABORTING");
      broadcastUpdate(txnId, "ABORTING");

      if (pauseAfterDecisionMs > 0) {
        await sleep(pauseAfterDecisionMs);
      }

      // STEP 4: ROLLBACK (best-effort). Missing participants are reconciled during recovery.
      const rollbackResults = await sendPhase2("rollback", txnId);
      if (hasPhase2Failures(rollbackResults)) {
        console.warn(`Rollback acknowledgements incomplete for ${txnId}; marking aborted and relying on recovery retries.`);
      }

      updateState(txnId, "ABORTED");
      broadcastUpdate(txnId, "ABORTED");

      writeLog({ transaction_id: txnId, state: "END" });

      return res.status(409).json({
        transaction_id: txnId,
        status: "ABORTED",
        reason: "Prepare phase failed",
      });
    }
  } catch (err) {
    console.error("Transaction Error:", err.message);

    return res.status(500).json({
      error: "Internal server error",
    });
  }
});

app.post("/chaos/kill", async (req, res) => {
  const requestedService = String(req.body?.service_name || "").trim();
  const serviceName = CHAOS_SERVICE_MAP[requestedService];

  if (!CHAOS_ALLOWLIST.has(requestedService)) {
    return res.status(400).json({
      error: "Unsupported service_name. Allowed values: inventory, orders, inventory-service, orders-service",
    });
  }

  try {
    const stopResult = await stopComposeService(serviceName);
    return res.status(200).json({
      status: stopResult.alreadyStopped ? "ALREADY_STOPPED" : "OK",
      service_name: requestedService,
      container_id: stopResult.containerId,
    });
  } catch (error) {
    return res.status(500).json({
      error: error.message,
    });
  }
});

app.post("/api/admin/pause-after-decision", (req, res) => {
  const delayMs = Number(req.body?.delay_ms);

  if (!Number.isFinite(delayMs) || delayMs < 0 || delayMs > 120000) {
    return res.status(400).json({
      error: "delay_ms must be a number between 0 and 120000",
    });
  }

  pauseAfterDecisionMs = delayMs;
  return res.status(200).json({
    status: "OK",
    pause_after_decision_ms: pauseAfterDecisionMs,
  });
});

/**
 * GET transaction status
 */
app.get("/api/transactions/:id", (req, res) => {
  const txnId = req.params.id;
  const status = transactions[txnId] || resolveStatusFromWal(txnId);

  return res.json({
    transaction_id: txnId,
    status,
  });
});

app.get("/", (req, res) => {
  res.json({ service: "coordinator", status: "UP" });
});

/**
 * START SERVER (IMPORTANT FIX)
 */
const server = app.listen(3000, () => {
  console.log("Coordinator running on port 3000");
  recoverFromWal().catch((error) => {
    console.error("WAL recovery failed:", error.message);
  });
});

/**
 * WEBSOCKET SERVER
 */
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  console.log("WebSocket client connected");

  clients.push(ws);

  ws.on("close", () => {
    clients = clients.filter((c) => c !== ws);
  });
});