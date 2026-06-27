# Aster Mint Detector — Build Prompt

> Standalone Ethereum mainnet NFT volume surge detection and Telegram alerting bot.
> Written in Python, deployed on Railway, structured for eventual merge into Aster Intelligence Bot.

---

## Project Overview

Build a bot called **Aster Mint Detector** that monitors the Ethereum mainnet in real time for NFT volume surges — detecting hot mints (public or allowlist) by watching `Transfer` events from the zero address across both ERC-721 and ERC-1155 contracts. When a surge is detected, the bot enriches the data and sends a live-updating alert to a single private Telegram channel.

No user access control is required. The bot serves one operator.

---

## Codebase Structure

```
aster-mint-detector/
├── main.py
├── config.py
├── detector.py
├── enricher.py
├── alerter.py
├── requirements.txt
├── Procfile
└── .env.example
```

---

## Module Responsibilities

### `config.py` — Configuration & Constants

- Loads all environment variables via `python-dotenv`
- Defines the default surge threshold: **20 mints / 60s**
- Defines the mint-ended velocity floor: **3 mints / 60s**
- Holds a `WATCHED_WALLETS` list — a Python list of Ethereum addresses representing known alpha wallets. Leave as an empty list with a clearly commented placeholder block:

```python
WATCHED_WALLETS = [
    # Add known alpha wallet addresses here
    # "0xABC...",
    # "0xDEF...",
]
```

- Holds RPC provider URLs (Alchemy primary, QuickNode fallback) as separate named constants
- Holds Etherscan API key, Alchemy NFT API key, Telegram bot token, and operator chat ID

---

### `detector.py` — Surge Detection Engine

Runs two concurrent async watchers via `asyncio`:

**Confirmed log watcher**
- Subscribes to `eth_subscribe("logs")` over WebSocket
- Filters `Transfer` events (ERC-721) where `topics[1]` is the zero address `0x000...000`
- Filters `TransferSingle` and `TransferBatch` events (ERC-1155) where the `from` field is the zero address
- Groups mint events by contract address in a sliding 60-second in-memory window

**Mempool watcher**
- Subscribes to `eth_subscribe("newPendingTransactions")`
- Fetches transaction data for previously unseen contract addresses
- Provides 5–15s early warning before block confirmation

**Surge logic**
- Fires a surge event when mint count within the window crosses the configured threshold **AND** at least 8 unique minter wallets are involved
- Re-triggers a surge event on a previously alerted contract only after velocity has dropped below the mint-ended floor and a new threshold crossing occurs
- Passes all surge events to `enricher.py` before alerting

**RPC fallback**
- Alchemy WebSocket is primary; QuickNode WebSocket is fallback
- On connection drop or error, automatically reconnects to the fallback provider
- Logs which provider is active at all times
- Reconnect uses exponential backoff: starts at 2s, caps at 60s
- If both providers fail simultaneously, logs a CRITICAL error and retries both on the backoff schedule

---

### `enricher.py` — Data Enrichment Layer

Triggered after every surge event. Fetches the following and returns a structured enrichment dict:

| Source | Data Fetched |
|---|---|
| Alchemy NFT API | Contract name, symbol, total minted supply, token standard (ERC-721 / ERC-1155) |
| Etherscan API | Verification status, deployment timestamp (for contract age), deployer wallet address |
| On-chain fallback | `eth_call` on `name()` and `symbol()` if Alchemy NFT API returns empty |

**Wallet enrichment**
- Checks each minter address in the current surge window against `WATCHED_WALLETS`
- If one or more watched wallets are present, sets `watched_wallet_hit: True` and includes the list of matching addresses in the dict

**Error handling**
- Returns `"Unknown"` or `None` gracefully for any field that fails to fetch
- Enrichment failure must never block an alert from sending

---

### `alerter.py` — Telegram Alert System

Uses **aiogram 3.x**. All commands and alerts are restricted to the single operator chat ID defined in config. Any other sender is silently ignored.

**Initial alert format**

```
⚡ SURGE DETECTED — [Contract Name or "Unknown"]

Contract: 0xABCD...1234
Standard: ERC-721 | ERC-1155
Verified: ✅ / ❌
Age: X hours / X days
Deployer: 0xDEF...5678

Mints (60s): 23
Unique Minters: 19
Total Supply Minted: 47

🔶 ALPHA WALLET ACTIVE: 0xABC...    ← only shown if watched_wallet_hit is True

[Etherscan] [OpenSea] [Blur]

🟢 Monitoring live...
```

**Live update behaviour**
- Edits the same Telegram message every 30 seconds with updated mint count, unique minter count, and total supply
- Keeps the channel clean — no new messages while a mint is active
- Stops updating when mint velocity drops below **3 mints / 60s**
- On mint end, edits the final message to replace `🟢 Monitoring live...` with `🔴 Mint activity ended`
- If message editing fails (e.g. message too old), posts a new follow-up message instead of crashing

**Telegram commands**

| Command | Description |
|---|---|
| `/status` | Active RPC provider, contracts being monitored, current surge threshold |
| `/threshold <number>` | Updates surge threshold in-memory immediately. Example: `/threshold 25` |
| `/watchlist` | Lists all addresses currently in `WATCHED_WALLETS` |
| `/addwatch <address>` | Adds an address to the in-memory watched wallet list for the current session |
| `/removewatch <address>` | Removes an address from the in-memory watched wallet list for the current session |

> `/addwatch` and `/removewatch` are session-only. The permanent list lives in `config.py`.

---

## Environment Variables

```env
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
ALCHEMY_WS_URL=wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
QUICKNODE_WS_URL=wss://your-endpoint.quiknode.pro/YOUR_KEY
ALCHEMY_NFT_API_KEY=
ETHERSCAN_API_KEY=
SURGE_THRESHOLD=20
```

---

## Dependencies

`requirements.txt` must include:

```
aiogram==3.x
web3
aiohttp
python-dotenv
```

---

## Deployment

- `Procfile` must contain: `worker: python main.py`
- No HTTP server, no FastAPI — this is a pure worker process
- All secrets loaded via environment variables, never hardcoded
- Deploy as a single Railway service with worker dyno type

---

## Error Handling & Resilience

- All external API calls (Etherscan, Alchemy NFT API) wrapped in `try/except` with logged failures
- Enrichment failure must never block an alert from sending
- WebSocket disconnection triggers automatic reconnect with exponential backoff (2s → 4s → 8s → ... → 60s cap)
- If both Alchemy and QuickNode WebSocket connections fail simultaneously, log CRITICAL and retry both on the backoff schedule
- All errors logged with timestamps to stdout (Railway captures this as logs)

---

## Merge Readiness

When this bot is eventually merged into Aster Intelligence Bot:

| File | Merges Into |
|---|---|
| `detector.py` | `aster/modules/surge_detector.py` |
| `enricher.py` | Aster scoring engine input (dict schema must remain compatible) |
| `alerter.py` | Aster bot instance + channel routing |
| `config.py` | Aster central config |

The enricher output dict should be designed from day one to be compatible with Aster's existing scoring engine input schema.

---

*Hand this prompt to your implementation LLM as-is. Populate `WATCHED_WALLETS` in `config.py` and fill in `.env` values before deployment.*
