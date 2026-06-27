"""
alerter.py — Aster Mint Detector
Telegram alert system using aiogram 3.x with interactive inline keyboards,
command auto-completion menus, and rich telemetry dashboards.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

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
# { contract_address: {"chat_id": int, "message_id": int, "ended": bool, "muted": bool} }
_active_alerts: dict[str, dict] = {}

# Reference injected from detector at runtime so alerter can report /status info
_detector_ref = None  # type: ignore[assignment]


def set_detector_ref(detector) -> None:  # noqa: ANN001
    """Called by main.py to give alerter a reference to the detector state."""
    global _detector_ref
    _detector_ref = detector


# ---------------------------------------------------------------------------
# Message & Keyboard formatting helpers
# ---------------------------------------------------------------------------

def _short_addr(addr: str | None) -> str:
    if not addr or len(addr) < 10:
        return addr or "Unknown"
    return f"{addr[:6]}...{addr[-4:]}"


def _verified_icon(verified: bool | None) -> str:
    if verified is True:
        return "✅ Verified"
    if verified is False:
        return "❌ Unverified"
    return "❓ Unknown"


def _build_alert_markup(contract_address: str, live: bool = True, muted: bool = False) -> InlineKeyboardMarkup:
    """Build interactive inline keyboard buttons for surge alerts."""
    etherscan_url = f"https://etherscan.io/address/{contract_address}"
    opensea_url = f"https://opensea.io/assets/ethereum/{contract_address}"
    blur_url = f"https://blur.io/collection/{contract_address}"

    buttons = [
        [
            InlineKeyboardButton(text="🔍 Etherscan", url=etherscan_url),
            InlineKeyboardButton(text="⛵ OpenSea", url=opensea_url),
            InlineKeyboardButton(text="⚡ Blur", url=blur_url),
        ]
    ]
    if live:
        short_id = contract_address[:10]
        mute_text = "🔔 Unmute Alert" if muted else "🔕 Mute Alert"
        buttons.append([
            InlineKeyboardButton(text="🔄 Refresh Stats", callback_data=f"ref:{short_id}"),
            InlineKeyboardButton(text=mute_text, callback_data=f"mute:{short_id}"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _build_alert_text(enrichment: dict, live: bool = True) -> str:
    """Build the full Telegram message string with rich formatting."""
    addr = enrichment["contract_address"]
    name = enrichment.get("name") or "Unknown Project"
    standard = enrichment.get("standard") or "Unknown Standard"
    verified_str = _verified_icon(enrichment.get("verified"))
    age = enrichment.get("contract_age_str") or "Unknown"
    deployer = enrichment.get("deployer")
    mint_count = enrichment.get("mint_count", 0)
    unique_minters = enrichment.get("unique_minters", 0)
    total_minted = enrichment.get("total_minted")
    watched_hit = enrichment.get("watched_wallet_hit", False)
    watched_addrs = enrichment.get("watched_wallet_addresses", [])

    # Calculate visual velocity bar
    max_threshold = max(config.SURGE_THRESHOLD, 1)
    ratio = min(mint_count / max_threshold, 2.0)
    filled_blocks = min(int(ratio * 5), 10)
    bar = "█" * filled_blocks + "░" * (10 - filled_blocks)

    lines = [
        f"⚡ <b>SURGE DETECTED</b> — <b>{name}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"📍 <b>Contract:</b> <code>{_short_addr(addr)}</code>",
        f"📜 <b>Standard:</b> {standard} | {verified_str}",
        f"⏱️ <b>Contract Age:</b> {age}",
    ]

    if deployer:
        lines.append(f"👨‍💻 <b>Deployer:</b> <code>{_short_addr(deployer)}</code>")

    lines.extend([
        "",
        f"🔥 <b>Mint Velocity ({config.WINDOW_SECONDS}s):</b> <code>[{bar}]</code> <b>{mint_count}</b>",
        f"👥 <b>Unique Minters:</b> <b>{unique_minters} buyers</b>",
    ])

    if total_minted is not None:
        lines.append(f"📦 <b>Total Supply Minted:</b> {total_minted:,}")

    if watched_hit and watched_addrs:
        lines.append("")
        short_addrs = ", ".join(f"<code>{_short_addr(a)}</code>" for a in watched_addrs[:3])
        lines.append(f"🔶 <b>ALPHA WALLETS ACTIVE:</b> {short_addrs}")

    lines.append("")
    lines.append("🟢 <b>Status:</b> Monitoring live mint stream..." if live else "🔴 <b>Status:</b> Mint activity ended")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public alert API (called by detector.py)
# ---------------------------------------------------------------------------

async def send_alert(enrichment: dict) -> None:
    """Send the initial surge alert message and register it for live updates."""
    contract_address = enrichment["contract_address"].lower()
    text = _build_alert_text(enrichment, live=True)
    markup = _build_alert_markup(contract_address, live=True)
    try:
        msg = await bot.send_message(
            chat_id=config.TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=markup,
        )
        _active_alerts[contract_address] = {
            "chat_id": msg.chat.id,
            "message_id": msg.message_id,
            "ended": False,
            "muted": False,
            "enrichment": enrichment,
        }
        logger.info("Alert sent for %s (msg_id=%s)", contract_address, msg.message_id)
    except Exception as exc:
        logger.error("Failed to send alert for %s: %s", contract_address, exc)


async def update_alert(contract_address: str, updated_enrichment: dict) -> None:
    """Edit the existing alert message with fresh mint counts."""
    key = contract_address.lower()
    state = _active_alerts.get(key)
    if not state or state["ended"] or state.get("muted", False):
        return

    state["enrichment"].update(updated_enrichment)
    text = _build_alert_text(state["enrichment"], live=True)
    markup = _build_alert_markup(key, live=True, muted=state.get("muted", False))
    await _edit_or_repost(key, state, text, markup)


async def end_alert(contract_address: str, final_enrichment: dict) -> None:
    """Transition the alert to 'Mint activity ended'."""
    key = contract_address.lower()
    state = _active_alerts.get(key)
    if not state:
        return

    state["enrichment"].update(final_enrichment)
    state["ended"] = True
    text = _build_alert_text(state["enrichment"], live=False)
    markup = _build_alert_markup(key, live=False, muted=False)
    await _edit_or_repost(key, state, text, markup)
    logger.info("Alert ended for %s", key)


async def _edit_or_repost(key: str, state: dict, text: str, markup: InlineKeyboardMarkup) -> None:
    """Edit an existing message; fall back to a new message if edit fails."""
    try:
        await bot.edit_message_text(
            chat_id=state["chat_id"],
            message_id=state["message_id"],
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=markup,
        )
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        logger.warning("Edit failed for %s (%s) — reposting", key, exc)
        try:
            msg = await bot.send_message(
                chat_id=config.TELEGRAM_CHAT_ID,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=markup,
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
# Interactive Callback Query Handlers (Inline Buttons)
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("ref:"))
async def cb_refresh_stats(callback: CallbackQuery) -> None:
    short_id = callback.data.split(":")[1] if callback.data else ""
    target_key = next((k for k in _active_alerts if k.startswith(short_id)), None)
    if target_key and target_key in _active_alerts:
        state = _active_alerts[target_key]
        text = _build_alert_text(state["enrichment"], live=not state["ended"])
        markup = _build_alert_markup(target_key, live=not state["ended"], muted=state.get("muted", False))
        await _edit_or_repost(target_key, state, text, markup)
        await callback.answer("🔄 Telemetry refreshed!", show_alert=False)
    else:
        await callback.answer("Alert session ended.", show_alert=False)


@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute_alert(callback: CallbackQuery) -> None:
    short_id = callback.data.split(":")[1] if callback.data else ""
    target_key = next((k for k in _active_alerts if k.startswith(short_id)), None)
    if target_key and target_key in _active_alerts:
        state = _active_alerts[target_key]
        state["muted"] = not state.get("muted", False)
        status_msg = "🔕 Alert muted for live edits." if state["muted"] else "🔔 Alert unmuted."
        markup = _build_alert_markup(target_key, live=not state["ended"], muted=state["muted"])
        if callback.message:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=callback.message.chat.id,
                    message_id=callback.message.message_id,
                    reply_markup=markup,
                )
            except Exception:
                pass
        await callback.answer(status_msg, show_alert=True)
    else:
        await callback.answer("Alert session expired.", show_alert=False)


# ---------------------------------------------------------------------------
# Command Handlers
# ---------------------------------------------------------------------------

@dp.message(Command("start"))
@dp.message(Command("help"))
async def cmd_start_help(message: Message) -> None:
    if not _is_operator(message):
        return

    text = (
        "🚀 <b>Aster Mint Detector — Console</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Welcome, Operator! Monitoring Ethereum Mainnet for real-time NFT mint surges.\n\n"
        "<b>Available Operator Commands:</b>\n"
        "📊 /status — Live system health & telemetry\n"
        "⚙️ /threshold <code>&lt;number&gt;</code> — Dynamically adjust surge threshold\n"
        "👀 /watchlist — View active alpha watched wallets\n"
        "➕ /addwatch <code>&lt;address&gt;</code> — Add alpha wallet to monitor\n"
        "➖ /removewatch <code>&lt;address&gt;</code> — Remove alpha wallet\n\n"
        "<i>Use the menu buttons below for quick telemetry access:</i>"
    )
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 System Status", callback_data="ref_status"),
            InlineKeyboardButton(text="👀 Watchlist", callback_data="ref_watchlist"),
        ]
    ])
    await message.answer(text, parse_mode="HTML", reply_markup=markup)


@dp.callback_query(F.data == "ref_status")
async def cb_menu_status(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await cmd_status(callback.message)


@dp.callback_query(F.data == "ref_watchlist")
async def cb_menu_watchlist(callback: CallbackQuery) -> None:
    await callback.answer()
    if callback.message:
        await cmd_watchlist(callback.message)


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
        "📊 <b>Aster Mint Detector — System Telemetry</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🌐 <b>Active RPC Provider:</b> <code>{provider}</code>\n"
        f"🔍 <b>Contracts Monitored:</b> <code>{monitored_contracts}</code>\n"
        f"🚨 <b>Active Live Surges:</b> <code>{active_alerts_count}</code>\n\n"
        "<b>Engine Parameters:</b>\n"
        f"⚡ <b>Surge Threshold:</b> <code>{config.SURGE_THRESHOLD}</code> mints / {config.WINDOW_SECONDS}s\n"
        f"👥 <b>Min Unique Buyers:</b> <code>{config.MIN_UNIQUE_MINTERS}</code>\n"
        f"📉 <b>Velocity Floor:</b> <code>{config.VELOCITY_FLOOR}</code> mints / {config.WINDOW_SECONDS}s\n"
        f"👀 <b>Watched Alpha Wallets:</b> <code>{len(config.WATCHED_WALLETS)}</code>\n\n"
        f"🕒 <b>System Time (UTC):</b> <code>{datetime.now(timezone.utc).strftime('%H:%M:%S')}</code>"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("threshold"))
async def cmd_threshold(message: Message) -> None:
    if not _is_operator(message):
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Usage: /threshold <number>\nExample: /threshold 15")
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

    lines = ["👀 <b>Watched Alpha Wallets:</b>", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for addr in config.WATCHED_WALLETS:
        lines.append(f"  • <code>{addr}</code>")
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
# Setup Bot Menu Commands
# ---------------------------------------------------------------------------

async def setup_bot_commands() -> None:
    """Configure Telegram UI command menu auto-suggestions."""
    commands = [
        BotCommand(command="status", description="Check system health & telemetry"),
        BotCommand(command="threshold", description="Update surge threshold"),
        BotCommand(command="watchlist", description="View monitored alpha wallets"),
        BotCommand(command="addwatch", description="Add wallet to watchlist"),
        BotCommand(command="removewatch", description="Remove wallet from watchlist"),
        BotCommand(command="help", description="Show operator console menu"),
    ]
    try:
        await bot.set_my_commands(commands)
        logger.info("Telegram bot commands set successfully.")
    except Exception as exc:
        logger.warning("Failed to set bot commands: %s", exc)


# ---------------------------------------------------------------------------
# Bot runner (called from main.py via asyncio.gather)
# ---------------------------------------------------------------------------

async def run_bot() -> None:
    """Start aiogram polling. Retries on transient network errors."""
    logger.info("Starting Telegram bot polling...")
    await setup_bot_commands()
    backoff = 2.0
    while True:
        try:
            await dp.start_polling(bot, handle_signals=False)
            break  # clean exit
        except Exception as exc:
            logger.error(
                "Telegram polling error: %s — retrying in %.0fs", exc, backoff
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
