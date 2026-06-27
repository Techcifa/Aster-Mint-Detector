# Aster Mint Detector

> Real-time Ethereum NFT volume surge detection and Telegram alerting bot.  
> Monitors ERC-721 and ERC-1155 mint events on Ethereum mainnet and fires live-updating alerts to a private Telegram channel when a surge is detected.

---

## How It Works

1. **detector.py** subscribes to Ethereum `Transfer` / `TransferSingle` / `TransferBatch` events over WebSocket
2. When ≥ 20 mints from ≥ 8 unique wallets hit within 60 seconds, a surge is triggered
3. **enricher.py** fetches contract metadata (name, standard, verification, deployer, age) from Alchemy + Etherscan
4. **alerter.py** posts a formatted alert to your Telegram channel and edits it live every 30 seconds
5. When mint velocity drops below 3/60s the message is updated to `🔴 Mint activity ended`

---

## Local Setup

```bash
git clone <your-repo>
cd aster-mint-detector

cp .env.example .env
# Fill in all values in .env

pip install -r requirements.txt
python main.py
```

To find your `OPERATOR_USER_ID`, run:
```bash
python check_chat_id.py
```
(Send any message to the bot first, then run the script.)

---

## Environment Variables

| Variable | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | From [@BotFather](https://t.me/BotFather) |
| `TELEGRAM_CHAT_ID` | Your operator channel chat ID (negative number, e.g. `-1001234567890`) |
| `OPERATOR_USER_ID` | Your personal Telegram user ID — authorises bot commands |
| `ALCHEMY_WS_URL` | Alchemy Ethereum mainnet WebSocket URL |
| `QUICKNODE_WS_URL` | QuickNode fallback WebSocket URL (leave blank if unused) |
| `ALCHEMY_NFT_API_KEY` | Alchemy API key (same key as in the WS URL) |
| `ETHERSCAN_API_KEY` | Etherscan API key |
| `SURGE_THRESHOLD` | Mints/60s to trigger alert (default: `20`) |

---

## Railway Deployment

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USER/aster-mint-detector.git
git push -u origin main
```

### 2. Create Railway Project
1. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
2. Select your repository

### 3. Set Environment Variables
In your Railway project → **Variables** tab, add every variable from `.env.example` with your real values.

> ⚠️ Do **not** commit your `.env` file. It is already excluded by `.gitignore`.

### 4. Verify Deployment
- Railway will auto-detect `railway.toml` and run `python main.py`
- Check the **Logs** tab — you should see:
  ```
  Aster Mint Detector starting up...
  Running in Alchemy-only mode ...
  Connected to Alchemy
  Run polling for bot @Aster_Mint_Detector_Bot
  ```
- Send `/status` to the bot in Telegram to confirm commands work

---

## Bot Commands

| Command | Description |
|---|---|
| `/status` | Active RPC, contracts monitored, live alerts, current threshold |
| `/threshold <n>` | Update surge threshold in-memory (e.g. `/threshold 25`) |
| `/watchlist` | List all watched alpha wallet addresses |
| `/addwatch <address>` | Add a wallet to the watchlist (session only) |
| `/removewatch <address>` | Remove a wallet from the watchlist (session only) |

> `/addwatch` and `/removewatch` are session-only. Edit `WATCHED_WALLETS` in `config.py` for permanent changes.

---

## Project Structure

```
aster-mint-detector/
├── main.py          # Entry point — runs detector + bot concurrently
├── config.py        # Env vars + constants
├── detector.py      # WebSocket log watcher + surge state machine
├── enricher.py      # Alchemy + Etherscan data enrichment
├── alerter.py       # Telegram alert system (aiogram 3.x)
├── requirements.txt
├── Procfile         # worker: python main.py
├── railway.toml     # Railway config-as-code
├── .python-version  # Pins Python 3.11 for Railway
├── .env.example     # Template — copy to .env and fill in values
└── .gitignore
```
