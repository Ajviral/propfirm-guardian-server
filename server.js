/**
 * PropFirm Guardian Server
 * ------------------------
 * Express HTTP API + WebSocket server for MT5 account sync.
 *
 * The MT5 Expert Advisor POSTs account snapshots to /api/account-update.
 * The server stores the latest snapshot per token and pushes live updates
 * to mobile app clients subscribed over WebSocket.
 */

const express = require('express');
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const cors = require('cors');

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/** Port — Railway and most PaaS hosts inject PORT automatically. */
const PORT = process.env.PORT || 3000;

/** RevenueCat secret API key — server-side only (never ship to mobile clients). */
const REVENUECAT_SECRET_KEY =
  process.env.REVENUECAT_SECRET_KEY || 'sk_TPVDZMTpneIpLKDNiNOgryKSLPZSu';

/**
 * RevenueCat project ID for V2 REST API.
 * Set REVENUECAT_PROJECT_ID on Railway when available; falls back to V1 subscribers API.
 */
const REVENUECAT_PROJECT_ID = process.env.REVENUECAT_PROJECT_ID || '';

const PRO_ENTITLEMENT_ID = 'pro';
const SUBSCRIPTION_CACHE_TTL_MS = 5 * 60 * 1000;

// ---------------------------------------------------------------------------
// In-memory storage
// ---------------------------------------------------------------------------

/**
 * Latest account snapshot keyed by token string.
 * Value shape matches the full payload from the MT5 EA plus server metadata.
 * @type {Map<string, object>}
 */
const accountDataStore = new Map();

/**
 * WebSocket clients subscribed per token.
 * Each token maps to a Set of open WebSocket connections.
 * @type {Map<string, Set<WebSocket>>}
 */
const subscribedClients = new Map();

/**
 * Maps MT5 connection token → RevenueCat app user id (from mobile register-token).
 * @type {Map<string, { revenueCatUserId: string; platform: string; registeredAt: number }>}
 */
const tokenUserMap = new Map();

/**
 * Cached subscription validation results per RevenueCat user id.
 * @type {Map<string, { active: boolean; at: number }>}
 */
const subscriptionCache = new Map();

// ---------------------------------------------------------------------------
// Express + HTTP server setup
// ---------------------------------------------------------------------------

const app = express();

// Allow cross-origin requests from the mobile app, MT5 WebRequest, and dev tools.
app.use(cors());

// Parse JSON bodies for POST /api/account-update.
app.use(express.json());
app.use(express.text({ type: '*/*' }));

// Shared HTTP server — Express and WebSocket listen on the same port.
const server = http.createServer(app);

// WebSocket server attached to the HTTP server (single PORT for both protocols).
const wss = new WebSocketServer({ server });

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/**
 * Normalize request body from Express — handles JSON objects, raw strings from
 * MQL5 WebRequest (which may not send Content-Type: application/json), etc.
 *
 * @param {unknown} body - req.body as received by Express
 * @returns {object} Parsed payload object, or {} if unparseable
 */
function parseBody(body) {
  if (!body) return {};
  if (typeof body === 'object') return body;
  if (typeof body === 'string') {
    try { return JSON.parse(body); } catch { return {}; }
  }
  return {};
}

/**
 * Broadcast an account update to every WebSocket client subscribed to a token.
 *
 * @param {string} token - Account token that was updated
 * @param {object} data - Full account snapshot to send
 */
function broadcastToSubscribers(token, data) {
  const clients = subscribedClients.get(token);

  if (!clients || clients.size === 0) {
    console.log(`[WS] No subscribers for token "${token}" — skipping broadcast`);
    return;
  }

  const message = JSON.stringify({
    type: 'accountUpdate',
    data,
  });

  let sentCount = 0;

  for (const client of clients) {
    // readyState 1 === WebSocket.OPEN — only send to live connections.
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
      sentCount++;
    }
  }

  console.log(
    `[WS] Broadcast accountUpdate for token "${token}" to ${sentCount} client(s)`,
  );
}

/**
 * Subscribe a WebSocket client to updates for a given token.
 * Also sends the latest stored snapshot immediately if one exists.
 *
 * @param {WebSocket} ws - Connected WebSocket client
 * @param {string} token - Account token to subscribe to
 */
function subscribeClient(ws, token) {
  if (!subscribedClients.has(token)) {
    subscribedClients.set(token, new Set());
  }

  subscribedClients.get(token).add(ws);

  // Track subscriptions on the socket so we can clean up on disconnect.
  if (!ws.subscribedTokens) {
    ws.subscribedTokens = new Set();
  }
  ws.subscribedTokens.add(token);

  console.log(`Client subscribed to token: ${token}`);

  // Send cached data immediately so the client doesn't wait for the next EA push.
  const existing = accountDataStore.get(token);
  if (existing && ws.readyState === WebSocket.OPEN) {
    ws.send(
      JSON.stringify({
        type: 'accountUpdate',
        data: existing,
      }),
    );
  }
}

/**
 * Remove a WebSocket client from all token subscription sets on disconnect.
 *
 * @param {WebSocket} ws - Disconnecting WebSocket client
 */
function unsubscribeClient(ws) {
  if (!ws.subscribedTokens) return;

  for (const token of ws.subscribedTokens) {
    const clients = subscribedClients.get(token);
    if (clients) {
      clients.delete(ws);
      // Drop empty sets to avoid memory leaks for unused tokens.
      if (clients.size === 0) {
        subscribedClients.delete(token);
      }
    }
  }

  ws.subscribedTokens.clear();
}

/**
 * Returns true when a RevenueCat entitlement is active (lifetime or not expired).
 * @param {object | undefined} entitlement
 */
function isEntitlementActive(entitlement) {
  if (!entitlement) return false;
  if (entitlement.expires_date == null) return true;
  return new Date(entitlement.expires_date) > new Date();
}

/**
 * Validates `pro` entitlement via RevenueCat REST API (5-minute cache per user).
 * Uses V2 when REVENUECAT_PROJECT_ID is set; otherwise V1 subscribers endpoint.
 *
 * @param {string} revenueCatUserId
 * @returns {Promise<boolean>}
 */
async function validateSubscription(revenueCatUserId) {
  const cached = subscriptionCache.get(revenueCatUserId);
  if (cached && Date.now() - cached.at < SUBSCRIPTION_CACHE_TTL_MS) {
    return cached.active;
  }

  let active = false;

  try {
    let url;
    if (REVENUECAT_PROJECT_ID) {
      url = `https://api.revenuecat.com/v2/projects/${REVENUECAT_PROJECT_ID}/customers/${encodeURIComponent(revenueCatUserId)}`;
    } else {
      url = `https://api.revenuecat.com/v1/subscribers/${encodeURIComponent(revenueCatUserId)}`;
    }

    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${REVENUECAT_SECRET_KEY}`,
        'Content-Type': 'application/json',
      },
    });

    if (!res.ok) {
      console.error('[RevenueCat] API error:', res.status, revenueCatUserId);
      subscriptionCache.set(revenueCatUserId, { active: false, at: Date.now() });
      return false;
    }

    const data = await res.json();

    if (REVENUECAT_PROJECT_ID) {
      const items = data.active_entitlements?.items ?? data.items ?? [];
      active = items.some(
        (item) =>
          item.entitlement_id === PRO_ENTITLEMENT_ID ||
          item.entitlement_identifier === PRO_ENTITLEMENT_ID ||
          item.lookup_key === PRO_ENTITLEMENT_ID,
      );
      const entitlements = data.entitlements?.items ?? [];
      if (!active) {
        active = entitlements.some(
          (item) =>
            (item.entitlement_id === PRO_ENTITLEMENT_ID ||
              item.lookup_key === PRO_ENTITLEMENT_ID) &&
            (item.is_active === true || item.expires_at == null),
        );
      }
    } else {
      active = isEntitlementActive(data.subscriber?.entitlements?.[PRO_ENTITLEMENT_ID]);
    }
  } catch (err) {
    console.error('[RevenueCat] validateSubscription failed:', err.message);
    active = false;
  }

  subscriptionCache.set(revenueCatUserId, { active, at: Date.now() });
  return active;
}

/**
 * Enforces subscription for registered tokens. Unregistered tokens are allowed
 * (dev/testing) with a warning log.
 *
 * @param {string} token
 * @returns {Promise<{ ok: true } | { ok: false; status: number; body: object }>}
 */
async function enforceTokenSubscription(token) {
  const mapping = tokenUserMap.get(token);

  if (!mapping?.revenueCatUserId) {
    console.warn(`Warning: unregistered token ${token}`);
    return { ok: true };
  }

  const active = await validateSubscription(mapping.revenueCatUserId);
  if (!active) {
    return {
      ok: false,
      status: 403,
      body: { success: false, error: 'Subscription inactive' },
    };
  }

  return { ok: true };
}

/**
 * Shared handler for MT5 account push payloads (both POST routes).
 */
async function handleAccountPush(parsed, res, logPrefix) {
  const {
    token,
    accountNumber,
    accountName,
    accountServer,
    accountCurrency,
    leverage,
    balance,
    equity,
    margin,
    freeMargin,
    floatingPnL,
    marginLevel,
    positions,
    timestamp,
  } = parsed;

  if (!token || !accountNumber) {
    return res.status(400).json({
      success: false,
      error: 'Missing token or accountNumber',
    });
  }

  const subscriptionCheck = await enforceTokenSubscription(token);
  if (!subscriptionCheck.ok) {
    return res.status(subscriptionCheck.status).json(subscriptionCheck.body);
  }

  const data = {
    token,
    accountNumber,
    accountName,
    accountServer,
    accountCurrency,
    leverage,
    balance,
    equity,
    margin,
    freeMargin,
    floatingPnL,
    marginLevel,
    positions: positions ?? [],
    timestamp,
    receivedAt: Date.now(),
  };

  accountDataStore.set(token, data);
  broadcastToSubscribers(token, data);

  console.log(
    `${logPrefix} token: ${token} Balance: ${balance} Equity: ${equity}`,
  );

  return res.status(200).json({
    success: true,
    message: 'Data received',
  });
}

// ---------------------------------------------------------------------------
// HTTP routes
// ---------------------------------------------------------------------------

/**
 * POST /api/register-token
 * Links an MT5 connection token to a RevenueCat customer id from the mobile app.
 */
app.post('/api/register-token', (req, res) => {
  const parsed = parseBody(req.body);
  const { token, revenueCatUserId, platform } = parsed;

  if (!token || !revenueCatUserId) {
    return res.status(400).json({
      success: false,
      error: 'Missing token or revenueCatUserId',
    });
  }

  tokenUserMap.set(token, {
    revenueCatUserId,
    platform: platform ?? 'unknown',
    registeredAt: Date.now(),
  });

  console.log(`[Register] token ${token} → RevenueCat user ${revenueCatUserId}`);

  return res.status(200).json({ success: true });
});

/**
 * POST /api/account-update
 * Primary ingestion endpoint called by PropFirmGuardianEA.mq5.
 */
app.post('/api/account-update', async (req, res) => {
  console.log('Raw body type:', typeof req.body, 'Body:', JSON.stringify(req.body).substring(0, 200));
  const parsed = parseBody(req.body);
  return handleAccountPush(parsed, res, 'Data received for');
});

app.post('/', async (req, res) => {
  console.log('Raw body type:', typeof req.body, 'Body:', JSON.stringify(req.body).substring(0, 200));
  const parsed = parseBody(req.body);
  return handleAccountPush(parsed, res, 'Root route - Data received for');
});

/**
 * GET /api/account/:token
 * Returns the latest stored snapshot — REST fallback when WebSocket is unavailable.
 */
app.get('/api/account/:token', (req, res) => {
  const { token } = req.params;
  const accountData = accountDataStore.get(token);

  if (!accountData) {
    return res.status(404).json({
      success: false,
      error: 'Account not found',
    });
  }

  return res.status(200).json({
    success: true,
    data: accountData,
  });
});

/**
 * GET /health
 * Liveness probe for Railway and load balancers.
 */
app.get('/health', (_req, res) => {
  res.status(200).json({
    status: 'ok',
    timestamp: Date.now(),
    connectedClients: wss.clients.size,
  });
});

// ---------------------------------------------------------------------------
// WebSocket connection handler
// ---------------------------------------------------------------------------

wss.on('connection', (ws) => {
  console.log(`WebSocket client connected. Total: ${wss.clients.size}`);

  // Greet the client so it knows the connection is live.
  ws.send(
    JSON.stringify({
      type: 'connected',
      message: 'PropFirm Guardian Server',
    }),
  );

  ws.on('message', (rawMessage) => {
    try {
      const message = JSON.parse(rawMessage.toString());

      if (message.type === 'subscribe' && message.token) {
        subscribeClient(ws, message.token);
      }
    } catch (err) {
      console.error('[WS] Failed to parse client message:', err.message);
    }
  });

  ws.on('close', () => {
    unsubscribeClient(ws);
    console.log('WebSocket client disconnected');
  });

  ws.on('error', (err) => {
    console.error('[WS] Client error:', err.message);
  });
});

// ---------------------------------------------------------------------------
// Start listening
// ---------------------------------------------------------------------------

server.listen(PORT, () => {
  console.log(`PropFirm Guardian Server running on port ${PORT}`);
});
