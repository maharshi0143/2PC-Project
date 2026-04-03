const express = require("express");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const axios = require("axios");

const app = express();
app.use(express.json());

// PostgreSQL connection
const pool = new Pool({
  host: "orders-db",
  user: "user",
  password: "password",
  database: "orders_db",
  port: 5432,
});

const COORDINATOR_URL = process.env.COORDINATOR_URL || "http://coordinator:3000";
const TXN_STATE_FILE = process.env.TXN_STATE_FILE || "./data/orders-transactions.json";

function ensureTxnStateFile() {
  fs.mkdirSync(path.dirname(TXN_STATE_FILE), { recursive: true });
  if (!fs.existsSync(TXN_STATE_FILE)) {
    fs.writeFileSync(TXN_STATE_FILE, "{}", "utf8");
  }
}

function readTxnState() {
  ensureTxnStateFile();
  try {
    return JSON.parse(fs.readFileSync(TXN_STATE_FILE, "utf8"));
  } catch (_error) {
    return {};
  }
}

function writeTxnState(state) {
  ensureTxnStateFile();
  fs.writeFileSync(TXN_STATE_FILE, JSON.stringify(state, null, 2), "utf8");
}

function upsertTxnState(transactionId, patch) {
  const current = readTxnState();
  current[transactionId] = {
    ...(current[transactionId] || {}),
    ...patch,
    updated_at: new Date().toISOString(),
  };
  writeTxnState(current);
}

function getTxnState(transactionId) {
  const current = readTxnState();
  return current[transactionId];
}

// In-memory transaction store
const transactions = {};

async function commitTransaction(transactionId) {
  const activeTxn = transactions[transactionId];
  const persistedTxn = getTxnState(transactionId);

  if (!persistedTxn || persistedTxn.status === "COMMITTED") {
    return;
  }

  if (persistedTxn.status !== "PREPARED") {
    return;
  }

  if (activeTxn?.client) {
    await activeTxn.client.query(
      "UPDATE orders SET status = $1 WHERE order_id = $2",
      ["CONFIRMED", activeTxn.orderId]
    );

    await activeTxn.client.query("COMMIT");
    activeTxn.client.release();
    delete transactions[transactionId];

    upsertTxnState(transactionId, {
      status: "COMMITTED",
      order_id: activeTxn.orderId,
    });
    return;
  }

  // Recovery path for PREPARED transactions after restart.
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const orderInsert = await client.query(
      "INSERT INTO orders (status) VALUES ($1) RETURNING order_id",
      ["CONFIRMED"]
    );

    const orderId = orderInsert.rows[0].order_id;
    for (const item of persistedTxn.items || []) {
      await client.query(
        "INSERT INTO order_items (order_id, sku, quantity) VALUES ($1, $2, $3)",
        [orderId, item.sku, item.quantity]
      );
    }

    await client.query("COMMIT");
    upsertTxnState(transactionId, {
      status: "COMMITTED",
      order_id: orderId,
    });
  } catch (error) {
    await client.query("ROLLBACK");
    upsertTxnState(transactionId, {
      status: "ABORTED",
      error: error.message,
    });
    throw error;
  } finally {
    client.release();
  }
}

async function rollbackTransaction(transactionId) {
  const activeTxn = transactions[transactionId];
  const persistedTxn = getTxnState(transactionId);

  if (!persistedTxn || persistedTxn.status === "ABORTED") {
    return;
  }

  if (activeTxn?.client) {
    await activeTxn.client.query("ROLLBACK");
    activeTxn.client.release();
    delete transactions[transactionId];
  }

  upsertTxnState(transactionId, { status: "ABORTED" });
}

async function reconcilePreparedTransactions() {
  const state = readTxnState();
  const preparedEntries = Object.entries(state).filter(([, txn]) => txn.status === "PREPARED");

  if (preparedEntries.length === 0) {
    return;
  }

  console.log(`Orders reconciliation found ${preparedEntries.length} PREPARED transaction(s)`);

  for (const [transactionId] of preparedEntries) {
    try {
      const response = await axios.get(
        `${COORDINATOR_URL}/api/transactions/${transactionId}`,
        { timeout: 5000 }
      );

      const status = response.data?.status;
      if (status === "COMMITTED" || status === "COMMITTING") {
        await commitTransaction(transactionId);
      } else {
        await rollbackTransaction(transactionId);
      }
    } catch (error) {
      console.error(`Orders reconciliation skipped ${transactionId}:`, error.message);
    }
  }
}

/**
 * PREPARE
 */
app.post("/prepare", async (req, res) => {
  const { transaction_id, items } = req.body;

  console.log("ORDERS PREPARE:", transaction_id);
  const persistedTxn = getTxnState(transaction_id);

  // Idempotency
  if (
    transactions[transaction_id]?.status === "PREPARED" ||
    persistedTxn?.status === "PREPARED"
  ) {
    return res.sendStatus(200);
  }

  if (persistedTxn?.status === "ABORTED") {
    return res.status(400).json({ error: persistedTxn.error || "Transaction already aborted" });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Create order
    const orderResult = await client.query(
      "INSERT INTO orders (status) VALUES ($1) RETURNING order_id",
      ["PENDING"]
    );

    const orderId = orderResult.rows[0].order_id;

    // Insert order items
    for (const item of items) {
      await client.query(
        "INSERT INTO order_items (order_id, sku, quantity) VALUES ($1, $2, $3)",
        [orderId, item.sku, item.quantity]
      );
    }

    // Save transaction
    transactions[transaction_id] = {
      client,
      orderId,
      items,
      status: "PREPARED",
    };
    upsertTxnState(transaction_id, {
      status: "PREPARED",
      order_id: orderId,
      items,
    });

    console.log("ORDERS PREPARED:", transaction_id);

    return res.sendStatus(200);
  } catch (err) {
    console.error("ORDERS PREPARE FAILED:", err.message);

    await client.query("ROLLBACK");
    client.release();

    upsertTxnState(transaction_id, {
      status: "ABORTED",
      error: err.message,
    });

    return res.status(400).json({ error: err.message });
  }
});

/**
 * COMMIT
 */
app.post("/commit", async (req, res) => {
  const { transaction_id } = req.body;

  console.log("ORDERS COMMIT:", transaction_id);

  try {
    await commitTransaction(transaction_id);

    console.log("ORDERS COMMITTED:", transaction_id);

    return res.sendStatus(200);
  } catch (err) {
    console.error("ORDERS COMMIT FAILED:", err.message);
    await rollbackTransaction(transaction_id);

    return res.status(500).json({ error: err.message });
  }
});

/**
 * ROLLBACK
 */
app.post("/rollback", async (req, res) => {
  const { transaction_id } = req.body;

  console.log("ORDERS ROLLBACK:", transaction_id);

  try {
    await rollbackTransaction(transaction_id);
    console.log("ORDERS ROLLED BACK:", transaction_id);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }

  return res.sendStatus(200);
});

app.get("/", (_req, res) => {
  res.send("Orders service running");
});

app.listen(3002, () => {
  console.log("Orders service running on port 3002");
  reconcilePreparedTransactions().catch((error) => {
    console.error("Orders reconciliation failed:", error.message);
  });
});