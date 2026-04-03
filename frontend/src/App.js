import { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

const IN_FLIGHT_STATUSES = new Set(["PREPARING", "COMMITTING", "ABORTING"]);
const TERMINAL_STATUS_PATH = {
  COMMITTED: ["PREPARING", "COMMITTING", "COMMITTED"],
  ABORTED: ["PREPARING", "ABORTING", "ABORTED"],
};
const STATUS_HOLD_MS = {
  PREPARING: 1200,
  COMMITTING: 1200,
  ABORTING: 1200,
};
const PHASE_SPEED_MULTIPLIER = {
  fast: 0.6,
  normal: 1,
  slow: 1.8,
};
const API_BASE = process.env.REACT_APP_API_URL || "http://localhost:3000";
const WS_URL =
  process.env.REACT_APP_WS_URL ||
  API_BASE.replace(/^http/i, "ws");
const DEFAULT_DEVELOPER_MODE = String(process.env.REACT_APP_ENABLE_CHAOS || "").toLowerCase() === "true";

function normalizeSku(rawSku) {
  return String(rawSku || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_-]/g, "")
    .slice(0, 64);
}

function toPositiveInt(rawValue) {
  const parsed = Number(rawValue);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function transactionSorter(a, b) {
  return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
}

function tryParseJson(text) {
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function App() {
  const [items, setItems] = useState([{ sku: "SKU001", quantity: 1 }]);
  const [transactions, setTransactions] = useState({});
  const [submitState, setSubmitState] = useState({ loading: false, error: "" });
  const [chaosService, setChaosService] = useState("inventory-service");
  const [developerMode, setDeveloperMode] = useState(DEFAULT_DEVELOPER_MODE);
  const [phaseSpeed, setPhaseSpeed] = useState("normal");
  const [wsState, setWsState] = useState({ connected: false, reconnectAttempt: 0 });
  const wsRef = useRef(null);
  const setTransactionRef = useRef(() => {});
  const resetAndPlayRef = useRef(() => {});
  const transactionsRef = useRef({});
  const statusQueueRef = useRef({});
  const statusTimerRef = useRef({});
  const phaseSpeedRef = useRef(phaseSpeed);

  const transactionList = useMemo(
    () => Object.values(transactions).sort(transactionSorter),
    [transactions]
  );

  useEffect(() => {
    transactionsRef.current = transactions;
  }, [transactions]);

  useEffect(() => {
    phaseSpeedRef.current = phaseSpeed;
  }, [phaseSpeed]);

  const applyTransactionStatus = (transactionId, status, timestamp = new Date().toISOString()) => {
    setTransactions((prev) => {
      const existing = prev[transactionId] || {
        transaction_id: transactionId,
        status,
        updatedAt: timestamp,
        history: [],
      };

      const history = [...existing.history];
      if (history.length === 0 || history[history.length - 1].status !== status) {
        history.push({ status, timestamp });
      }

      return {
        ...prev,
        [transactionId]: {
          ...existing,
          status,
          updatedAt: timestamp,
          history,
        },
      };
    });
  };

  const processStatusQueue = (transactionId) => {
    if (statusTimerRef.current[transactionId]) {
      return;
    }

    const queue = statusQueueRef.current[transactionId] || [];
    const nextStatus = queue.shift();
    if (!nextStatus) {
      return;
    }

    applyTransactionStatus(transactionId, nextStatus.status, nextStatus.timestamp);

    const speedKey = phaseSpeedRef.current;
    const speedMultiplier = PHASE_SPEED_MULTIPLIER[speedKey] || 1;
    const holdMs = Math.round((STATUS_HOLD_MS[nextStatus.status] || 0) * speedMultiplier);
    if (holdMs > 0) {
      statusTimerRef.current[transactionId] = setTimeout(() => {
        delete statusTimerRef.current[transactionId];
        processStatusQueue(transactionId);
      }, holdMs);
      return;
    }

    processStatusQueue(transactionId);
  };

  const enqueueStatuses = (transactionId, statuses, timestamp = new Date().toISOString()) => {
    if (!statusQueueRef.current[transactionId]) {
      statusQueueRef.current[transactionId] = [];
    }

    const queue = statusQueueRef.current[transactionId];
    for (const status of statuses) {
      const lastQueued = queue[queue.length - 1];
      if (!lastQueued || lastQueued.status !== status) {
        queue.push({ status, timestamp });
      }
    }

    processStatusQueue(transactionId);
  };

  const resetAndPlayTerminalSequence = (
    transactionId,
    terminalStatus,
    timestamp = new Date().toISOString()
  ) => {
    const sequence = TERMINAL_STATUS_PATH[terminalStatus];
    if (!sequence) {
      return;
    }

    const activeTimer = statusTimerRef.current[transactionId];
    if (activeTimer) {
      clearTimeout(activeTimer);
      delete statusTimerRef.current[transactionId];
    }

    statusQueueRef.current[transactionId] = [];
    enqueueStatuses(transactionId, sequence, timestamp);
  };
  resetAndPlayRef.current = resetAndPlayTerminalSequence;

  const setTransaction = (transactionId, status, timestamp = new Date().toISOString()) => {
    const existing = transactionsRef.current[transactionId];
    const currentStatus = existing?.status;
    let statusesToQueue = [status];

    const terminalPath = TERMINAL_STATUS_PATH[status];
    if (!existing && terminalPath) {
      statusesToQueue = terminalPath;
    } else if (status === "COMMITTED" && currentStatus === "PREPARING") {
      statusesToQueue = ["COMMITTING", "COMMITTED"];
    } else if (status === "ABORTED" && currentStatus === "PREPARING") {
      statusesToQueue = ["ABORTING", "ABORTED"];
    }

    const lastHistoryStatus = existing?.history?.[existing.history.length - 1]?.status;
    if (lastHistoryStatus) {
      statusesToQueue = statusesToQueue.filter((nextStatus, index) => {
        if (index === 0 && nextStatus === lastHistoryStatus) {
          return false;
        }
        return true;
      });
    }

    if (statusesToQueue.length > 0) {
      enqueueStatuses(transactionId, statusesToQueue, timestamp);
    }
  };
  setTransactionRef.current = setTransaction;

  useEffect(() => {
    let timeoutId;

    const connectSocket = (attempt = 0) => {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        setWsState({ connected: true, reconnectAttempt: 0 });
      };

      ws.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          if (
            message?.type === "TRANSACTION_UPDATE" &&
            message?.payload?.transaction_id &&
            message?.payload?.status
          ) {
            if (message.payload.status === "COMMITTED" || message.payload.status === "ABORTED") {
              resetAndPlayRef.current(
                message.payload.transaction_id,
                message.payload.status,
                message.payload.timestamp
              );
            } else {
              setTransactionRef.current(
                message.payload.transaction_id,
                message.payload.status,
                message.payload.timestamp
              );
            }
          }
        } catch (error) {
          console.error("Invalid WebSocket message", error);
        }
      };

      ws.onclose = () => {
        const nextAttempt = attempt + 1;
        setWsState({ connected: false, reconnectAttempt: nextAttempt });
        const waitMs = Math.min(1000 * 2 ** attempt, 10000);
        timeoutId = setTimeout(() => connectSocket(nextAttempt), waitMs);
      };

      ws.onerror = () => {
        ws.close();
      };
    };

    connectSocket();

    return () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      if (wsRef.current && wsRef.current.readyState <= 1) {
        wsRef.current.close();
      }

      for (const timer of Object.values(statusTimerRef.current)) {
        clearTimeout(timer);
      }
      statusTimerRef.current = {};
      statusQueueRef.current = {};
    };
  }, []);

  const onItemChange = (index, field, value) => {
    setItems((prev) =>
      prev.map((item, currentIndex) => {
        if (currentIndex !== index) {
          return item;
        }

        if (field === "sku") {
          return { ...item, sku: normalizeSku(value) };
        }

        return { ...item, quantity: value.replace(/[^0-9]/g, "") };
      })
    );
  };

  const addItemRow = () => {
    setItems((prev) => [...prev, { sku: "", quantity: 1 }]);
  };

  const removeItemRow = (index) => {
    setItems((prev) => prev.filter((_, currentIndex) => currentIndex !== index));
  };

  const submitTransaction = async (event) => {
    event.preventDefault();
    setSubmitState({ loading: true, error: "" });

    const normalizedItems = items
      .map((item) => ({
        sku: normalizeSku(item.sku),
        quantity: toPositiveInt(item.quantity),
      }))
      .filter((item) => item.sku && item.quantity);

    if (normalizedItems.length === 0) {
      setSubmitState({
        loading: false,
        error: "Add at least one valid item with SKU and quantity > 0.",
      });
      return;
    }

    const controller = new AbortController();
    const abortTimer = setTimeout(() => controller.abort(), 12000);

    try {
      const response = await fetch(`${API_BASE}/api/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ items: normalizedItems }),
        signal: controller.signal,
      });

      const text = await response.text();
      const payload = tryParseJson(text);

      if (!response.ok) {
        if (payload?.transaction_id && payload?.status) {
          if (payload.status === "COMMITTED" || payload.status === "ABORTED") {
            resetAndPlayTerminalSequence(payload.transaction_id, payload.status);
          } else {
            setTransaction(payload.transaction_id, payload.status);
          }
        }

        if (payload?.status === "ABORTED") {
          setItems([{ sku: "SKU001", quantity: 1 }]);
          setSubmitState({ loading: false, error: "" });
          return;
        }

        throw new Error(
          payload?.reason || payload?.error || text || "Transaction failed."
        );
      }

      if (!payload?.transaction_id || !payload?.status) {
        throw new Error("Coordinator returned an invalid response.");
      }

      if (payload.status === "COMMITTED" || payload.status === "ABORTED") {
        resetAndPlayTerminalSequence(payload.transaction_id, payload.status);
      } else {
        setTransaction(payload.transaction_id, payload.status);
      }

      setItems([{ sku: "SKU001", quantity: 1 }]);
    } catch (error) {
      const errorMessage =
        error.name === "AbortError"
          ? "Request timed out. Please retry."
          : error.message || "Failed to submit transaction.";

      setSubmitState({ loading: false, error: errorMessage });
      return;
    } finally {
      clearTimeout(abortTimer);
    }

    setSubmitState({ loading: false, error: "" });
  };

  const triggerChaos = async (transactionId) => {
    try {
      const response = await fetch(`${API_BASE}/chaos/kill`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ service_name: chaosService }),
      });

      if (!response.ok) {
        const text = await response.text();
        let message = "Chaos endpoint request failed.";

        if (text) {
          try {
            const payload = JSON.parse(text);
            if (payload?.error) {
              message = payload.error;
            }
          } catch {
            message = text;
          }
        }

        throw new Error(message);
      }

      const chaosTag = `${chaosService.toUpperCase().replace(/-/g, "_")}_CHAOS_TRIGGERED`;
      setTransaction(transactionId, chaosTag);
    } catch (error) {
      setSubmitState({
        loading: false,
        error: `Chaos request failed for ${transactionId}: ${error.message}`,
      });
    }
  };

  return (
    <main className="layout">
      <section className="hero">
        <h1>Distributed Transaction Monitor</h1>
        <p>
          Observe every 2PC state change in real time, submit checkout events, and
          trigger controlled failures to test resilience.
        </p>
        <div className="connection-row" aria-live="polite">
          <span className={`pill ${wsState.connected ? "online" : "offline"}`}>
            {wsState.connected ? "WebSocket Connected" : "WebSocket Reconnecting"}
          </span>
          {!wsState.connected && wsState.reconnectAttempt > 0 && (
            <span className="retries">Attempt #{wsState.reconnectAttempt}</span>
          )}
        </div>
      </section>

      <section className="panel form-panel">
        <div className="panel-header">
          <h2>Create Transaction</h2>
          <div className="phase-speed-control">
            <label htmlFor="phase-speed">Phase speed</label>
            <select
              id="phase-speed"
              value={phaseSpeed}
              onChange={(event) => setPhaseSpeed(event.target.value)}
            >
              <option value="fast">Fast</option>
              <option value="normal">Normal</option>
              <option value="slow">Slow</option>
            </select>
          </div>
        </div>
        <form onSubmit={submitTransaction} noValidate>
          {items.map((item, index) => (
            <div className="item-row" key={`item-${index}`}>
              <label>
                SKU
                <input
                  type="text"
                  inputMode="text"
                  maxLength={64}
                  value={item.sku}
                  onChange={(event) => onItemChange(index, "sku", event.target.value)}
                  placeholder="SKU001"
                  required
                />
              </label>
              <label>
                Quantity
                <input
                  type="text"
                  inputMode="numeric"
                  value={item.quantity}
                  onChange={(event) => onItemChange(index, "quantity", event.target.value)}
                  placeholder="1"
                  required
                />
              </label>
              {items.length > 1 && (
                <button
                  type="button"
                  className="muted-btn"
                  onClick={() => removeItemRow(index)}
                >
                  Remove
                </button>
              )}
            </div>
          ))}

          <div className="form-actions">
            <button type="button" className="muted-btn" onClick={addItemRow}>
              Add Item
            </button>
            <button type="submit" className="primary-btn" disabled={submitState.loading}>
              {submitState.loading ? "Submitting..." : "Run 2PC Transaction"}
            </button>
          </div>
          {submitState.error && (
            <p role="alert" className="error-text">
              {submitState.error}
            </p>
          )}
        </form>
      </section>

      <section className="panel transactions-panel">
        <div className="panel-header">
          <h2>Transaction Timeline</h2>
          <div className="timeline-controls">
            <label className="developer-mode-control" htmlFor="developer-mode">
              <input
                id="developer-mode"
                type="checkbox"
                checked={developerMode}
                onChange={(event) => setDeveloperMode(event.target.checked)}
              />
              Developer Mode
            </label>
            {developerMode && (
              <div className="chaos-config">
                <label htmlFor="chaos-service">Chaos target</label>
                <select
                  id="chaos-service"
                  value={chaosService}
                  onChange={(event) => setChaosService(event.target.value)}
                >
                  <option value="inventory-service">inventory-service</option>
                  <option value="orders-service">orders-service</option>
                </select>
              </div>
            )}
          </div>
        </div>

        <div className="transaction-list" data-testid="transaction-list">
          {transactionList.length === 0 && (
            <p className="empty-state">No transactions yet. Submit one to start monitoring.</p>
          )}

          {transactionList.map((transaction) => (
            <article
              key={transaction.transaction_id}
              className="transaction-item"
              data-testid={`transaction-item-${transaction.transaction_id}`}
            >
              <header>
                <h3>{transaction.transaction_id}</h3>
                <span
                  className={`status ${transaction.status.toLowerCase()}`}
                  data-testid={`transaction-status-${transaction.transaction_id}`}
                >
                  {transaction.status}
                </span>
              </header>

              <p className="updated-at">
                Updated: {new Date(transaction.updatedAt).toLocaleString()}
              </p>

              <div className="history">
                {transaction.history.map((event, index) => (
                  <span
                    key={`${transaction.transaction_id}-${event.status}-${index}`}
                    className={event.status.toLowerCase()}
                  >
                    {event.status}
                  </span>
                ))}
              </div>

              {developerMode && IN_FLIGHT_STATUSES.has(transaction.status) && (
                <button
                  type="button"
                  className="chaos-btn"
                  data-testid={`chaos-btn-${transaction.transaction_id}`}
                  onClick={() => triggerChaos(transaction.transaction_id)}
                >
                  Trigger Chaos
                </button>
              )}
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}

export default App;
