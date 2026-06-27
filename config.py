"""
config.py — Aster Mint Detector
Loads all environment variables and defines shared constants.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: int = int(os.environ["TELEGRAM_CHAT_ID"])

# Your personal Telegram user ID (from check_chat_id.py).
# Commands (/status, /threshold, etc.) are accepted from this user ID,
# allowing you to DM the bot directly in addition to the channel.
OPERATOR_USER_ID: int = int(os.getenv("OPERATOR_USER_ID", "0"))

# ---------------------------------------------------------------------------
# RPC Providers (WebSocket)
# ---------------------------------------------------------------------------
ALCHEMY_WS_URL: str = os.environ["ALCHEMY_WS_URL"]
QUICKNODE_WS_URL: str | None = os.getenv("QUICKNODE_WS_URL") or None

# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------
ALCHEMY_NFT_API_KEY: str = os.environ["ALCHEMY_NFT_API_KEY"]
ETHERSCAN_API_KEY: str = os.environ["ETHERSCAN_API_KEY"]

# ---------------------------------------------------------------------------
# Surge Detection Constants
# ---------------------------------------------------------------------------

# Mints within WINDOW_SECONDS to trigger a surge alert
SURGE_THRESHOLD: int = int(os.getenv("SURGE_THRESHOLD", "20"))

# Minimum unique minters required alongside the threshold crossing
MIN_UNIQUE_MINTERS: int = 8

# Rolling window duration in seconds
WINDOW_SECONDS: int = 60

# Velocity (mints/window) below which a mint is considered ended
VELOCITY_FLOOR: int = 3

# How often (seconds) the live Telegram alert message is edited during a surge
ALERT_UPDATE_INTERVAL: int = 30

# ---------------------------------------------------------------------------
# Watched Wallets
# ---------------------------------------------------------------------------
# Add known alpha wallet addresses here (checksummed or lowercase — both work).
# Changes here require a restart. Use /addwatch and /removewatch for session-only
# additions without restarting the bot.
WATCHED_WALLETS: list[str] = [
    # "0xABC...",
    # "0xDEF...",
]

# ---------------------------------------------------------------------------
# External API Base URLs
# ---------------------------------------------------------------------------
ALCHEMY_NFT_BASE_URL: str = (
    f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_NFT_API_KEY}"
)
ETHERSCAN_BASE_URL: str = "https://api.etherscan.io/api"
