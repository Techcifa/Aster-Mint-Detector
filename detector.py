"""
detector.py — Aster Mint Detector
Surge detection engine.

Two concurrent async tasks:
  1. Confirmed log watcher — eth_subscribe("logs") over WebSocket
  2. Mempool watcher    — eth_subscribe("newPendingTransactions")

Surge logic:
  - Fires when mint_count >= SURGE_THRESHOLD AND unique_minters >= MIN_UNIQUE_MINTERS
     within a rolling WINDOW_SECONDS window
  - State machine per contract: IDLE → SURGING → ENDED → IDLE
  - Re-alerts only after velocity drops below VELOCITY_FLOOR and threshold is crossed again

RPC fallback:
  - Alchemy is primary; QuickNode is fallback
  - Exponential backoff: 2s → 4s → 8s → … → 60s cap
  - CRITICAL log if both fail simultaneously
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque, defaultdict
from enum import Enum, auto
from typing import Deque

from web3 import AsyncWeb3
from web3.providers import WebSocketProvider

import config
import enricher
import alerter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ABI topic signatures (keccak256 of event sig)
# ---------------------------------------------------------------------------
ZERO_ADDRESS_PADDED = "0x" + "0" * 64

# ERC-721: Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
ERC721_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ERC-1155: TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)
ERC1155_TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"

# ERC-1155: TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)
ERC1155_TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"

ALL_TOPICS = [
    ERC721_TRANSFER_TOPIC,
    ERC1155_TRANSFER_SINGLE_TOPIC,
    ERC1155_TRANSFER_BATCH_TOPIC,
]
# Pre-lowercase for fast comparison
ALL_TOPICS_LOWER = {t.lower() for t in ALL_TOPICS}


# ---------------------------------------------------------------------------
# Topic & Subscription helpers
# ---------------------------------------------------------------------------

def _normalize_sub_id(sub_id: any) -> str:
    if sub_id is None:
        return ""
    if isinstance(sub_id, bytes):
        return sub_id.hex().lower()
    val = str(sub_id).lower()
    if val.startswith("0x"):
        return val
    return "0x" + val


def _topic_to_hex(topic: any) -> str:
    if topic is None:
        return ""
    if isinstance(topic, bytes):
        return "0x" + topic.hex()
    val = str(topic).lower()
    if not val.startswith("0x"):
        return "0x" + val
    return val


def _is_zero_address(topic: any) -> bool:
    if topic is None:
        return False
    try:
        hex_str = _topic_to_hex(topic)
        return int(hex_str, 16) == 0
    except (ValueError, TypeError):
        return False


def _extract_address_from_topic(topic: any) -> str:
    hex_str = _topic_to_hex(topic)
    if len(hex_str) >= 42:
        return "0x" + hex_str[-40:]
    return "0x0000000000000000000000000000000000000000"


def _parse_block_number(val: any) -> int | None:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, str):
        try:
            return int(val, 16) if val.startswith("0x") else int(val)
        except ValueError:
            return None
    return None



# ---------------------------------------------------------------------------
# Contract surge state machine
# ---------------------------------------------------------------------------

class SurgeState(Enum):
    IDLE = auto()
    SURGING = auto()
    ENDED = auto()


# Per-contract state
class ContractState:
    def __init__(self) -> None:
        # deque of (timestamp: float, minter_address: str)
        self.window: Deque[tuple[float, str]] = deque()
        self.state: SurgeState = SurgeState.IDLE
        # Track when we last sent an update to avoid flooding
        self.last_update_sent: float = 0.0


# ---------------------------------------------------------------------------
# Detector class
# ---------------------------------------------------------------------------

class Detector:
    def __init__(self) -> None:
        # Live per-contract windows — used by alerter /status command
        self.surge_windows: dict[str, ContractState] = defaultdict(ContractState)
        self.active_provider_name: str = "None"

        # Mempool: track pending tx hashes we've already fetched
        self._seen_pending: set[str] = set()

        # w3 instance shared between tasks (replaced on reconnect)
        self._w3: AsyncWeb3 | None = None

        # Backoff state
        self._backoff: float = 2.0

        # Provider ordering — QuickNode is included only if configured
        self._providers = [("Alchemy", config.ALCHEMY_WS_URL)]
        if config.QUICKNODE_WS_URL:
            self._providers.append(("QuickNode", config.QUICKNODE_WS_URL))
        if len(self._providers) == 1:
            logger.info("Running in Alchemy-only mode (no QuickNode fallback configured).")
        self._primary_index: int = 0  # which provider is "current"

        # Block tracking for historical reconnect catch-up
        self._last_processed_block: int | None = None

    # -----------------------------------------------------------------------
    # Public entry point
    # -----------------------------------------------------------------------

    async def run(self) -> None:
        """Main loop — reconnects indefinitely with exponential backoff."""
        alerter.set_detector_ref(self)
        provider_index = 0

        while True:
            name, url = self._providers[provider_index % len(self._providers)]
            self.active_provider_name = name
            logger.info("Connecting to %s WebSocket...", name)
            try:
                async with AsyncWeb3(WebSocketProvider(url)) as w3:
                    self._w3 = w3
                    self._backoff = 2.0  # reset on successful connect
                    logger.info("Connected to %s", name)
                    await asyncio.gather(
                        self._event_dispatcher(w3),
                        self._alert_updater(),
                        self._telemetry_heartbeat(),
                    )
            except Exception as exc:
                logger.error(
                    "%s connection error: %s — reconnecting in %.0fs",
                    name,
                    exc,
                    self._backoff,
                )
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 60.0)

                # Switch provider
                provider_index += 1
                next_name = self._providers[provider_index % len(self._providers)][0]

                # If we've tried all providers in this cycle, log CRITICAL
                if provider_index % len(self._providers) == 0:
                    logger.critical(
                        "All RPC providers failed. Retrying on backoff schedule (%.0fs).",
                        self._backoff,
                    )

                logger.info("Switching to %s", next_name)

    # -----------------------------------------------------------------------
    # Confirmed log watcher
    # -----------------------------------------------------------------------

    async def _confirmed_log_watcher(self, w3: AsyncWeb3) -> None:
        """Subscribe to eth_subscribe('logs') and process Transfer events."""
        logger.info("Starting confirmed log watcher on %s", self.active_provider_name)
        subscription_id = await w3.eth.subscribe(
            "logs",
            {
                "topics": [ALL_TOPICS],
            },
        )
        logger.info("Log subscription id: %s", subscription_id)

        async for payload in w3.socket.process_subscriptions():
            try:
                # web3.py >= 7 yields the log object directly (AttributeDict)
                # Earlier versions wrapped it in {"result": log}
                if hasattr(payload, "get"):
                    log = payload.get("result", payload)
                else:
                    log = payload
                if log is None:
                    continue
                await self._process_log(w3, log)
            except Exception as exc:
                logger.warning("Error processing log: %s", exc)

    async def _process_log(self, w3: AsyncWeb3, log) -> None:
        """Classify a log entry and record a mint if applicable.

        `log` may be an AttributeDict (web3.py >= 7) or a plain dict.
        We access fields via getattr-with-fallback to support both.
        """
        # Normalise to plain dict so .get() always works
        if not isinstance(log, dict):
            try:
                log = dict(log)
            except Exception:
                return

        block_num = _parse_block_number(log.get("blockNumber"))
        if block_num is not None:
            if self._last_processed_block is None or block_num > self._last_processed_block:
                self._last_processed_block = block_num

        topics = log.get("topics", [])
        if not topics:
            return

        topic0_raw = topics[0].hex() if isinstance(topics[0], bytes) else topics[0]
        topic0 = topic0_raw.lower()

        # Fast reject — skip events that are not any of the 3 mint signatures
        if topic0 not in ALL_TOPICS_LOWER:
            return

        contract_address = log.get("address", "").lower()
        if not contract_address:
            return

        minter: str | None = None
        mint_qty: int = 1

        # ERC-721 Transfer from zero address: Transfer(address from, address to, uint256 tokenId)
        if topic0 == ERC721_TRANSFER_TOPIC.lower() and len(topics) >= 2:
            if _is_zero_address(topics[1]):
                if len(topics) >= 3:
                    minter = _extract_address_from_topic(topics[2])
                else:
                    minter = "0x0000000000000000000000000000000000000000"

        # ERC-1155 TransferSingle — topics: [sig, operator, from, to] (all indexed)
        elif topic0 == ERC1155_TRANSFER_SINGLE_TOPIC.lower() and len(topics) >= 3:
            if _is_zero_address(topics[2]):
                if len(topics) >= 4:
                    minter = _extract_address_from_topic(topics[3])
                else:
                    minter = _extract_address_from_topic(topics[2])
                # Extract value (mint quantity) from log data
                data = log.get("data", "0x")
                if isinstance(data, bytes):
                    data_bytes = data
                elif isinstance(data, str) and data.startswith("0x"):
                    data_bytes = bytes.fromhex(data[2:])
                else:
                    data_bytes = b""
                if len(data_bytes) >= 64:
                    # id is first 32 bytes (0..32), value is second 32 bytes (32..64)
                    val = int.from_bytes(data_bytes[32:64], "big")
                    mint_qty = max(val, 1)

        # ERC-1155 TransferBatch — topics: [sig, operator, from, to] (all indexed)
        elif topic0 == ERC1155_TRANSFER_BATCH_TOPIC.lower() and len(topics) >= 3:
            if _is_zero_address(topics[2]):
                if len(topics) >= 4:
                    minter = _extract_address_from_topic(topics[3])
                else:
                    minter = _extract_address_from_topic(topics[2])
                # Count tokens from data: ids array length
                data = log.get("data", "0x")
                if isinstance(data, bytes):
                    data_bytes = data
                elif isinstance(data, str) and data.startswith("0x"):
                    data_bytes = bytes.fromhex(data[2:])
                else:
                    data_bytes = b""
                if len(data_bytes) >= 96:
                    ids_offset = int.from_bytes(data_bytes[0:32], "big")
                    if ids_offset + 32 <= len(data_bytes):
                        ids_length = int.from_bytes(
                            data_bytes[ids_offset: ids_offset + 32], "big"
                        )
                        mint_qty = max(ids_length, 1)

        if minter is None:
            return

        # Record in sliding window (mint_qty times for batch)
        now = time.monotonic()
        cs = self.surge_windows[contract_address]
        for _ in range(mint_qty):
            cs.window.append((now, minter))

        # Log every mint so activity is visible before surge threshold is crossed
        logger.info(
            "Mint detected: contract=%s minter=%s qty=%d window_size=%d",
            contract_address, minter, mint_qty, len(cs.window)
        )

        await self._check_surge(contract_address, w3)

    # -----------------------------------------------------------------------
    # Surge check
    # -----------------------------------------------------------------------

    async def _check_surge(self, contract_address: str, w3: AsyncWeb3) -> None:
        """Evict stale window entries and evaluate surge conditions."""
        now = time.monotonic()
        cutoff = now - config.WINDOW_SECONDS
        cs = self.surge_windows[contract_address]

        # Evict entries outside the window
        while cs.window and cs.window[0][0] < cutoff:
            cs.window.popleft()

        mint_count = len(cs.window)
        minters = [entry[1] for entry in cs.window]
        unique_minters = len(set(minters))
        velocity = mint_count  # mints within last WINDOW_SECONDS

        # Log progress every 5 mints so we can see the window filling
        if mint_count > 0 and mint_count % 5 == 0:
            logger.info(
                "[%s] window=%d mints / %d unique (threshold=%d / %d unique)",
                contract_address[:10],
                mint_count,
                unique_minters,
                config.SURGE_THRESHOLD,
                config.MIN_UNIQUE_MINTERS,
            )

        if cs.state == SurgeState.IDLE:
            if velocity >= config.SURGE_THRESHOLD and unique_minters >= config.MIN_UNIQUE_MINTERS:
                logger.info(
                    "SURGE on %s: %d mints, %d unique minters",
                    contract_address,
                    mint_count,
                    unique_minters,
                )
                cs.state = SurgeState.SURGING
                # Enrich and alert (non-blocking)
                asyncio.create_task(
                    self._enrich_and_alert(contract_address, minters, mint_count, w3)
                )

        elif cs.state == SurgeState.SURGING:
            if velocity < config.VELOCITY_FLOOR:
                logger.info("Mint ended on %s — velocity=%d", contract_address, velocity)
                cs.state = SurgeState.ENDED
                asyncio.create_task(
                    self._end_alert(contract_address, minters, mint_count, w3)
                )

        elif cs.state == SurgeState.ENDED:
            if velocity < config.VELOCITY_FLOOR:
                # Allow re-trigger once velocity is confirmed low
                cs.state = SurgeState.IDLE
                logger.debug("Contract %s reset to IDLE", contract_address)

    async def _enrich_and_alert(
        self,
        contract_address: str,
        minters: list[str],
        mint_count: int,
        w3: AsyncWeb3,
    ) -> None:
        try:
            enrichment = await enricher.enrich(contract_address, minters, mint_count, w3)
            await alerter.send_alert(enrichment)
        except Exception as exc:
            logger.error("Enrich+alert error for %s: %s", contract_address, exc)

    async def _end_alert(
        self,
        contract_address: str,
        minters: list[str],
        mint_count: int,
        w3: AsyncWeb3,
    ) -> None:
        try:
            enrichment = await enricher.enrich(contract_address, minters, mint_count, w3)
            await alerter.end_alert(contract_address, enrichment)
        except Exception as exc:
            logger.error("End-alert error for %s: %s", contract_address, exc)

    # -----------------------------------------------------------------------
    # Alert updater (30-second live edit loop)
    # -----------------------------------------------------------------------

    async def _alert_updater(self) -> None:
        """Continuously edit live Telegram alerts every ALERT_UPDATE_INTERVAL seconds."""
        while True:
            await asyncio.sleep(config.ALERT_UPDATE_INTERVAL)
            if self._w3 is None:
                continue
            for contract_address, cs in list(self.surge_windows.items()):
                if cs.state != SurgeState.SURGING:
                    continue
                # Build updated enrichment snapshot (lightweight, no API call)
                now = time.monotonic()
                cutoff = now - config.WINDOW_SECONDS
                recent = [(ts, m) for ts, m in cs.window if ts >= cutoff]
                mint_count = len(recent)
                minters = [m for _, m in recent]
                update = {
                    "contract_address": contract_address,
                    "mint_count": mint_count,
                    "unique_minters": len(set(minters)),
                }
                try:
                    await alerter.update_alert(contract_address, update)
                except Exception as exc:
                    logger.warning("Update alert error for %s: %s", contract_address, exc)

    async def _telemetry_heartbeat(self) -> None:
        """Periodically log system health and active monitoring stats every 60 seconds."""
        while True:
            await asyncio.sleep(60)
            active_contracts = len(self.surge_windows)
            active_surges = sum(1 for cs in self.surge_windows.values() if cs.state == SurgeState.SURGING)
            logger.info(
                "📡 Telemetry Pulse: Provider=%s | Monitored Contracts=%d | Active Surges=%d | Latest Block=%s",
                self.active_provider_name,
                active_contracts,
                active_surges,
                self._last_processed_block or "Connecting...",
            )

    # -----------------------------------------------------------------------
    # Unified event dispatcher (web3.py 7 compatible)
    # -----------------------------------------------------------------------

    async def _event_dispatcher(self, w3: AsyncWeb3) -> None:
        """Subscribe to logs + pending txs via a single process_subscriptions() loop.

        web3.py 7 exposes process_subscriptions() as a single shared async
        generator on the socket.  Running it in two concurrent tasks causes
        each task to steal the other's messages.  Instead we subscribe to both
        feeds here, record both subscription IDs, and dispatch each incoming
        event to the correct handler based on the subscription_id field.
        """
        logger.info("Starting event dispatcher on %s", self.active_provider_name)

        # Subscribe to confirmed Transfer logs
        log_sub_id = await w3.eth.subscribe(
            "logs",
            {"topics": [ALL_TOPICS]},
        )
        logger.info("Log subscription id: %s", log_sub_id)

        # Subscribe to pending transactions (best-effort — skip on failure)
        pending_sub_id = None
        try:
            pending_sub_id = await w3.eth.subscribe("newPendingTransactions")
            logger.info("Pending tx subscription id: %s", pending_sub_id)
        except Exception as exc:
            logger.warning("Mempool subscription failed: %s — running without mempool watcher", exc)

        # Historical catch-up on connection / reconnection
        try:
            current_block = await w3.eth.block_number
            if self._last_processed_block is not None and current_block > self._last_processed_block:
                from_block = self._last_processed_block + 1
                logger.info(
                    "Catching up historical logs from block %d to %d...",
                    from_block,
                    current_block,
                )
                past_logs = await w3.eth.get_logs({
                    "fromBlock": hex(from_block),
                    "toBlock": hex(current_block),
                    "topics": [ALL_TOPICS],
                })
                logger.info("Found %d historical logs during catch-up", len(past_logs))
                for past_log in past_logs:
                    await self._process_log(w3, past_log)
            self._last_processed_block = current_block
        except Exception as exc:
            logger.warning("Historical log catch-up error: %s", exc)

        norm_log_sub_id = _normalize_sub_id(log_sub_id)
        norm_pending_sub_id = _normalize_sub_id(pending_sub_id) if pending_sub_id else None

        async for payload in w3.socket.process_subscriptions():
            try:
                # web3.py 7 yields an AttributeDict with keys:
                #   subscription  -> subscription id (HexBytes or str)
                #   result        -> the actual event data
                if hasattr(payload, "subscription"):
                    sub_id = payload.subscription
                    result = payload.result
                elif isinstance(payload, dict):
                    sub_id = payload.get("subscription")
                    result = payload.get("result", payload)
                else:
                    # Raw HexBytes or unknown — skip
                    logger.debug("Unrecognised subscription payload type: %s", type(payload))
                    continue

                norm_sub_id = _normalize_sub_id(sub_id)

                if norm_sub_id == norm_log_sub_id:
                    await self._handle_log(w3, result)
                elif norm_pending_sub_id is not None and norm_sub_id == norm_pending_sub_id:
                    self._handle_pending_tx(result)
                else:
                    logger.debug("Unknown subscription id %s (norm: %s) — skipping", sub_id, norm_sub_id)

            except Exception as exc:
                logger.warning("Dispatcher error: %s", exc)

    async def _handle_log(self, w3: AsyncWeb3, log) -> None:
        """Process a confirmed Transfer log."""
        if log is None:
            return
        await self._process_log(w3, log)

    def _handle_pending_tx(self, tx_hash) -> None:
        """Track pending transaction hashes for mempool pre-warming."""
        if not tx_hash:
            return
        tx_hash_str = tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)
        if tx_hash_str in self._seen_pending:
            return
        self._seen_pending.add(tx_hash_str)
        # Limit memory
        if len(self._seen_pending) > 50_000:
            self._seen_pending.clear()
