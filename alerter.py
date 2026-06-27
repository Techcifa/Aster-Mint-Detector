"""
alerter.py — Aster Mint Detector
Telegram alert system using aiogram 3.x.

Responsibilities:
  - Send the initial surge alert message
  - Edit it every 30s with live mint counts while a mint is active
  - Transition the message to "Mint activity ended" when velocity drops
  - Handle 5 operator-only commands: /status, /threshold, /watchlist,
    /addwatch, /removewatch
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import Message

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level bot / dispatcher singletons
# ---------------------------------------------------------------------------

bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# ---------------------------------------------------------------------------
# Live alert state
# Keyed by contract_address (lowercase)
# ---------------------------------------------------------------------------
# { contract_address: {"chat_id": int, "message_id": int, "ended": bool} }
_active_alerts: dict[str, dict] = {}

# Reference injected from detector at runtime so alerter can report /status info
_detector_ref = None  # type: ignore[assignment]


def set_detector_ref(detector) -> None:  # noqa: ANN001
    """Called by main.py to give alerter a reference to the detector state."""
    global _detector_ref
    _detector_ref = detector


# ---------------------------------------------------------------------------
# Message formatting helpers
# ---------------------------------------------------------------------------

def _short_addr(addr: str | None) -> str:
    if not addr or len(addr) < 10:
        return addr or "Unknown"
    return f"{addr[:6]}...{addr[-4:]}"


def _verified_icon(verified: bool | None) -> str:
    if verified is True:
        return "✅"
    if verified is False:
        return "❌"
    return "❓"


def _build_alert_text(enrichment: dict, live: bool = True) -> str:
    """Build the full Telegram message string from an enrichment dict."""
    addr = enrichment["contract_address"]
    name = enrichment.get("name") or "Unknown"
    standard = enrichment.get("standard") or "Unknown"
    verified = enrichment.get("verified")
    age = enrichment.get("contract_age_str") or "Unknown"
    deployer = enrichment.get("deployer")
    mint_count = enrichment.get("mint_count", 0)
    unique_minters = enrichment.get("unique_minters", 0)
    total_minted = enrichment.get("total_minted")
    watched_hit = enrichment.get("watched_wallet_hit", False)
    watched_addrs = enrichment.get("watched_wallet_addresses", [])

    etherscan_url = f"https://etherscan.io/address/{addr}"
    opensea_url = f"https://opensea.io/assets/ethereum/{addr}"
    blur_url = f"https://blur.io/collection/{addr}"

    lines = [
        f"⚡ <b>SURGE DETECTED</b> — {name}",
        "",
        f"<b>Contract:</b> <code>{_short_addr(addr)}</code>",
        f"<b>Standard:</b> {standard}",
        f"<b>Verified:</b> {_verified_icon(verified)}",
        f"<b>Age:</b> {age}",
        f"<b>Deployer:</b> <code>{_short_addr(deployer)}</code>",
        "",
        f"<b>Mints (60s):</b> {mint_count}",
        f"<b>Unique Minters:</b> {unique_minters}",
    ]

    if total_minted is not None:
        lines.append(f"<b>Total Supply Minted:</b> {total_minted}")

    if watched_hit and watched_addrs:
        lines.append("")
        short_addrs = " | ".join(_short_addr(a) for a in watched_addrs[:3])
        lines.append(f"🔶 <b>ALPHA WALLET ACTIVE:</b> {short_addrs}")

    lines.append("")
    lines.append(
        f'<a href="{etherscan_url}">Etherscan</a> | '
        f'<a href="{opensea_url}">OpenSea</a> | '
        f'<a href="{blur_url}">Blur</a>'
    )
    lines.append("")
    lines.append("🟢 Monitoring live..." if live else "🔴 Mint activity ended")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public alert API (called by detector.py)
# ---------------------------------------------------------------------------

async def send_alert(enrichment: dict) -> None:
    """Send the initial surge alert message and register it for live updates."""
    contract_address = enrichment["contract_address"].lower()
    text = _build_alert_text(enrichment, live=True)
    try:
        msg = await bot.send_message(
            chat_id=config.TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        _active_alerts[contract_address] = {
            "chat_id": msg.chat.id,
            "message_id": msg.message_id,
            "ended": False,
            "enrichment": enrichment,  # kept so we can re-render on update
        }
        logger.info("Alert sent for %s (msg_id=%s)", contract_address, msg.message_id)
    except Exception as exc:
        logger.error("Failed to send alert for %s: %s", contract_address, exc)


async def update_alert(contract_address: str, updated_enrichment: dict) -> None:
    """Edit the existing alert message with fresh mint counts."""
    key = contract_address.lower()
    state = _active_alerts.get(key)
    if not state or state["ended"]:
        return

    state["enrichment"].update(updated_enrichment)
    text = _build_alert_text(state["enrichment"], live=True)
    await _edit_or_repost(key, state, text, live=True)


async def end_alert(contract_address: str, final_enrichment: dict) -> None:
    """Transition the alert to 'Mint activity ended'."""
    key = contract_address.lower()
    state = _active_alerts.get(key)
    if not state:
        return

    state["enrichment"].update(final_enrichment)
    state["ended"] = True
    text = _build_alert_text(state["enrichment"], live=False)
    await _edit_or_repost(key, state, text, live=False)
    logger.info("Alert ended for %s", key)


async def _edit_or_repost(key: str, state: dict, text: str, live: bool) -> None:
    """Edit an existing message; fall back to a new message if edit fails."""
    try:
        await bot.edit_message_text(
            chat_id=state["chat_id"],
            message_id=state["message_id"],
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except TelegramBadRequest as exc:
        logger.warning("Edit failed for %s (%s) — reposting", key, exc)
        try:
            msg = await bot.send_message(
                chat_id=config.TELEGRAM_CHAT_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            state["chat_id"] = msg.chat.id
            state["message_id"] = msg.message_id
        except Exception as send_exc:
            logger.error("Repost also failed for %s: %s", key, send_exc)
    except Exception as exc:
        logger.error("Unexpected edit error for %s: %s", key, exc)


# ---------------------------------------------------------------------------
# Operator guard
# ---------------------------------------------------------------------------

def _is_operator(message: Message) -> bool:
    """Returns True if the message is from the operator channel OR the operator's personal DM."""
    if message.chat.id == config.TELEGRAM_CHAT_ID:
        return True
    if config.OPERATOR_USER_ID and message.from_user and message.from_user.id == config.OPERATOR_USER_ID:
        return True
    return False


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

@dp.message(Command("status"))
async def cmd_status(message: Message) -> None:
    if not _is_operator(message):
        return

    provider = "Unknown"
    monitored_contracts = 0
    if _detector_ref is not None:
        provider = getattr(_detector_ref, "active_provider_name", "Unknown")
        monitored_contracts = len(getattr(_detector_ref, "surge_windows", {}))

    active_alerts_count = sum(1 for s in _active_alerts.values() if not s["ended"])
    text = (
        "📊 <b>Aster Mint Detector — Status</b>\n\n"
        f"<b>Active RPC:</b> {provider}\n"
        f"<b>Contracts in window:</b> {monitored_contracts}\n"
        f"<b>Live alerts:</b> {active_alerts_count}\n"
        f"<b>Surge threshold:</b> {config.SURGE_THRESHOLD} mints / {config.WINDOW_SECONDS}s\n"
        f"<b>Min unique minters:</b> {config.MIN_UNIQUE_MINTERS}\n"
        f"<b>Velocity floor:</b> {config.VELOCITY_FLOOR} mints / {config.WINDOW_SECONDS}s\n"
        f"<b>Watched wallets:</b> {len(config.WATCHED_WALLETS)}\n"
        f"<b>Time (UTC):</b> {datetime.now(timezone.utc).strftime('%H:%M:%S')}"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("threshold"))
async def cmd_threshold(message: Message) -> None:
    if not _is_operator(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /threshold <number>\nExample: /threshold 25")
        return

    try:
        new_threshold = int(parts[1].strip())
        if new_threshold < 1:
            raise ValueError("Must be >= 1")
    except ValueError as exc:
        await message.answer(f"❌ Invalid value: {exc}")
        return

    config.SURGE_THRESHOLD = new_threshold
    await message.answer(
        f"✅ Surge threshold updated to <b>{new_threshold}</b> mints/{config.WINDOW_SECONDS}s",
        parse_mode="HTML",
    )
    logger.info("Surge threshold updated to %s by operator", new_threshold)


@dp.message(Command("watchlist"))
async def cmd_watchlist(message: Message) -> None:
    if not _is_operator(message):
        return

    if not config.WATCHED_WALLETS:
        await message.answer("👀 Watched wallets: <i>(empty)</i>", parse_mode="HTML")
        return

    lines = ["👀 <b>Watched Wallets:</b>"]
    for addr in config.WATCHED_WALLETS:
        lines.append(f"  <code>{addr}</code>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("addwatch"))
async def cmd_addwatch(message: Message) -> None:
    if not _is_operator(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /addwatch <address>\nExample: /addwatch 0xABC...")
        return

    addr = parts[1].strip()
    if not addr.startswith("0x") or len(addr) != 42:
        await message.answer("❌ Invalid Ethereum address format.")
        return

    if addr.lower() in [w.lower() for w in config.WATCHED_WALLETS]:
        await message.answer(f"ℹ️ <code>{addr}</code> is already in the watchlist.", parse_mode="HTML")
        return

    config.WATCHED_WALLETS.append(addr)
    await message.answer(
        f"✅ Added <code>{addr}</code> to watched wallets (session only).\n"
        f"Total watched: {len(config.WATCHED_WALLETS)}",
        parse_mode="HTML",
    )
    logger.info("Operator added watch address: %s", addr)


@dp.message(Command("removewatch"))
async def cmd_removewatch(message: Message) -> None:
    if not _is_operator(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /removewatch <address>")
        return

    addr = parts[1].strip().lower()
    original = config.WATCHED_WALLETS[:]
    config.WATCHED_WALLETS[:] = [w for w in config.WATCHED_WALLETS if w.lower() != addr]

    if len(config.WATCHED_WALLETS) == len(original):
        await message.answer(f"ℹ️ Address not found in watchlist: <code>{addr}</code>", parse_mode="HTML")
    else:
        await message.answer(
            f"✅ Removed <code>{addr}</code> from watched wallets (session only).\n"
            f"Total watched: {len(config.WATCHED_WALLETS)}",
            parse_mode="HTML",
        )
        logger.info("Operator removed watch address: %s", addr)


# ---------------------------------------------------------------------------
# Bot runner (called from main.py via asyncio.gather)
# ---------------------------------------------------------------------------

async def run_bot() -> None:
    """Start aiogram polling. Runs indefinitely."""
    logger.info("Starting Telegram bot polling...")
    await dp.start_polling(bot, handle_signals=False)
