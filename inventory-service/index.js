const express = require("express");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const axios = require("axios");

const app = express();
app.use(express.json());

// PostgreSQL connection
const pool = new Pool({
  host: "inventory-db",
  user: "user",
  password: "password",
  database: "inventory_db",
  port: 5432,
});

const COORDINATOR_URL = process.env.COORDINATOR_URL || "http://coordinator:3000";
const TXN_STATE_FILE = process.env.TXN_STATE_FILE || "./data/inventory-transactions.json";

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
    for (const item of activeTxn.items) {
      await activeTxn.client.query(
        "UPDATE products SET stock_quantity = stock_quantity - $1 WHERE sku = $2",
        [item.quantity, item.sku]
      );
    }

    await activeTxn.client.query("COMMIT");
    activeTxn.client.release();
    delete transactions[transactionId];

    upsertTxnState(transactionId, { status: "COMMITTED" });
    return;
  }

  // Recovery path when service restarted after PREPARED.
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const item of persistedTxn.items || []) {
      const updated = await client.query(
        "UPDATE products SET stock_quantity = stock_quantity - $1 WHERE sku = $2 AND stock_quantity >= $1",
        [item.quantity, item.sku]
      );

      if (updated.rowCount === 0) {
        throw new Error(`Insufficient stock for ${item.sku} during recovery commit`);
      }
    }

    await client.query("COMMIT");
    upsertTxnState(transactionId, { status: "COMMITTED" });
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

  console.log(`Inventory reconciliation found ${preparedEntries.length} PREPARED transaction(s)`);

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
      console.error(`Inventory reconciliation skipped ${transactionId}:`, error.message);
    }
  }
}

/**
 * PREPARE
 */
app.post("/prepare", async (req, res) => {
  const { transaction_id, items } = req.body;
  const startedAt = Date.now();

  console.log("PREPARE received:", transaction_id);
  const persistedTxn = getTxnState(transaction_id);

  // Idempotency check
  if (
    transactions[transaction_id]?.status === "PREPARED" ||
    persistedTxn?.status === "PREPARED"
  ) {
    return res.sendStatus(200);
  }

  if (persistedTxn?.status === "ABORTED") {
    return res.status(400).json({
      error: persistedTxn.error || "Transaction already aborted",
    });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    // Avoid long lock waits if another transaction currently holds the row lock.
    await client.query("SET LOCAL lock_timeout = '1500ms'");

    for (const item of items) {
      const result = await client.query(
        "SELECT stock_quantity FROM products WHERE sku = $1 FOR UPDATE NOWAIT",
        [item.sku]
      );

      if (result.rows.length === 0) {
        throw new Error(`Product ${item.sku} not found`);
      }

      const stock = result.rows[0].stock_quantity;

      if (stock < item.quantity) {
        throw new Error(`Insufficient stock for ${item.sku}`);
      }
    }

    // Save transaction state
    transactions[transaction_id] = {
      client,
      items,
      status: "PREPARED",
    };
    upsertTxnState(transaction_id, {
      status: "PREPARED",
      items,
    });

    console.log("PREPARED:", transaction_id);
    console.log(`PREPARE completed in ${Date.now() - startedAt}ms:`, transaction_id);

    return res.sendStatus(200);
  } catch (err) {
    const errorMessage =
      err.code === "55P03"
        ? "Product is currently locked by another transaction; retry shortly"
        : err.message;

    console.error("PREPARE FAILED:", errorMessage);

    await client.query("ROLLBACK");
    client.release();

    upsertTxnState(transaction_id, {
      status: "ABORTED",
      error: errorMessage,
    });

    return res.status(400).json({ error: errorMessage });
  }
});

/**
 * COMMIT
 */
app.post("/commit", async (req, res) => {
  const { transaction_id } = req.body;

  console.log("COMMIT received:", transaction_id);

  try {
    await commitTransaction(transaction_id);

    console.log("COMMITTED:", transaction_id);

    return res.sendStatus(200);
  } catch (err) {
    console.error("COMMIT FAILED:", err.message);
    await rollbackTransaction(transaction_id);

    return res.status(500).json({ error: err.message });
  }
});

/**
 * ROLLBACK
 */
app.post("/rollback", async (req, res) => {
  const { transaction_id } = req.body;

  console.log("ROLLBACK received:", transaction_id);

  try {
    await rollbackTransaction(transaction_id);
    console.log("ROLLED BACK:", transaction_id);
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }

  return res.sendStatus(200);
});

app.get("/", (_req, res) => {
  res.send("Inventory service running");
});

app.listen(3001, () => {
  console.log("Inventory service running on port 3001");
  reconcilePreparedTransactions().catch((error) => {
    console.error("Inventory reconciliation failed:", error.message);
  });
});