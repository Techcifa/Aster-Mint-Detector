"""
enricher.py — Aster Mint Detector
Data enrichment layer: fetches contract metadata from Alchemy NFT API,
Etherscan, and on-chain eth_call fallbacks. Returns a stable EnrichmentDict
compatible with Aster Intelligence Bot's scoring engine input schema.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TypedDict

import aiohttp
from web3 import AsyncWeb3

import config

logger = logging.getLogger(__name__)

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# ABI fragments for on-chain fallback
_NAME_SIG = "0x06fdde03"    # name()
_SYMBOL_SIG = "0x95d89b41"  # symbol()


# ---------------------------------------------------------------------------
# Output type — designed for Aster Intelligence Bot merge compatibility
# ---------------------------------------------------------------------------

class EnrichmentDict(TypedDict):
    contract_address: str
    name: str                        # "Unknown" on failure
    symbol: str                      # "Unknown" on failure
    standard: str                    # "ERC-721" | "ERC-1155" | "Unknown"
    total_minted: int | None
    verified: bool | None
    deployed_at: str | None          # ISO 8601 UTC timestamp
    contract_age_str: str            # Human-readable: "3 hours" / "2 days"
    deployer: str | None
    watched_wallet_hit: bool
    watched_wallet_addresses: list[str]
    mint_count: int
    unique_minters: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _short_addr(addr: str) -> str:
    """Return a shortened address like 0xABCD...1234."""
    if not addr or len(addr) < 10:
        return addr
    return f"{addr[:6]}...{addr[-4:]}"


def _age_str(deployed_at_iso: str | None) -> str:
    """Return a human-readable contract age string."""
    if not deployed_at_iso:
        return "Unknown"
    try:
        deployed = datetime.fromisoformat(deployed_at_iso.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - deployed
        total_seconds = int(delta.total_seconds())
        if total_seconds < 3600:
            mins = total_seconds // 60
            return f"{mins} minute{'s' if mins != 1 else ''}"
        elif total_seconds < 86400:
            hours = total_seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''}"
        else:
            days = total_seconds // 86400
            return f"{days} day{'s' if days != 1 else ''}"
    except Exception:
        return "Unknown"


async def _alchemy_nft_metadata(
    session: aiohttp.ClientSession,
    contract_address: str,
) -> dict:
    """Fetch contract metadata from Alchemy NFT API v3."""
    url = f"{config.ALCHEMY_NFT_BASE_URL}/getContractMetadata"
    params = {"contractAddress": contract_address}
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                logger.warning("Alchemy NFT API returned %s for %s", resp.status, contract_address)
                return {}
            data = await resp.json()
            return data
    except Exception as exc:
        logger.warning("Alchemy NFT API error for %s: %s", contract_address, exc)
        return {}


async def _etherscan_source(
    session: aiohttp.ClientSession,
    contract_address: str,
) -> dict:
    """Fetch contract source info (verification + deployer) from Etherscan."""
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": contract_address,
        "apikey": config.ETHERSCAN_API_KEY,
    }
    try:
        async with session.get(
            config.ETHERSCAN_BASE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            if data.get("status") != "1" or not data.get("result"):
                return {}
            return data["result"][0]
    except Exception as exc:
        logger.warning("Etherscan source error for %s: %s", contract_address, exc)
        return {}


async def _etherscan_creation_tx(
    session: aiohttp.ClientSession,
    contract_address: str,
) -> dict:
    """Fetch the contract creation transaction to extract deployer and deploy time."""
    params = {
        "module": "contract",
        "action": "getcontractcreation",
        "contractaddresses": contract_address,
        "apikey": config.ETHERSCAN_API_KEY,
    }
    try:
        async with session.get(
            config.ETHERSCAN_BASE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
            if data.get("status") != "1" or not data.get("result"):
                return {}
            return data["result"][0]
    except Exception as exc:
        logger.warning("Etherscan creation tx error for %s: %s", contract_address, exc)
        return {}


async def _etherscan_block_time(
    session: aiohttp.ClientSession,
    tx_hash: str,
) -> str | None:
    """Resolve a tx hash to a block timestamp via Etherscan."""
    params = {
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": tx_hash,
        "apikey": config.ETHERSCAN_API_KEY,
    }
    try:
        async with session.get(
            config.ETHERSCAN_BASE_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            block_number_hex = (data.get("result") or {}).get("blockNumber")
            if not block_number_hex:
                return None
        # Now fetch block timestamp
        block_params = {
            "module": "proxy",
            "action": "eth_getBlockByNumber",
            "tag": block_number_hex,
            "boolean": "false",
            "apikey": config.ETHERSCAN_API_KEY,
        }
        async with session.get(
            config.ETHERSCAN_BASE_URL,
            params=block_params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp2:
            if resp2.status != 200:
                return None
            data2 = await resp2.json()
            ts_hex = (data2.get("result") or {}).get("timestamp")
            if not ts_hex:
                return None
            ts = int(ts_hex, 16)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception as exc:
        logger.warning("Etherscan block time error for tx %s: %s", tx_hash, exc)
        return None


async def _eth_call_string(w3: AsyncWeb3, contract_address: str, selector: str) -> str | None:
    """Call a no-arg string-returning function (name() or symbol()) on-chain."""
    try:
        target_addr = w3.to_checksum_address(contract_address) if hasattr(w3, "to_checksum_address") else contract_address
        result = await w3.eth.call({"to": target_addr, "data": selector})
        if not result or len(result) < 96:
            return None
        # ABI-decode: offset (32 bytes) + length (32 bytes) + data
        length = int.from_bytes(result[64:96], "big")
        if len(result) < 96 + length:
            return None
        return result[96:96 + length].decode("utf-8", errors="ignore").strip("\x00").strip()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

async def enrich(
    contract_address: str,
    minters: list[str],
    mint_count: int,
    w3: AsyncWeb3,
) -> EnrichmentDict:
    """
    Enrich a surge event with contract metadata and wallet intelligence.
    Never raises — all failures default gracefully to None / "Unknown".
    """
    result: EnrichmentDict = {
        "contract_address": contract_address,
        "name": "Unknown",
        "symbol": "Unknown",
        "standard": "Unknown",
        "total_minted": None,
        "verified": None,
        "deployed_at": None,
        "contract_age_str": "Unknown",
        "deployer": None,
        "watched_wallet_hit": False,
        "watched_wallet_addresses": [],
        "mint_count": mint_count,
        "unique_minters": len(set(minters)),
    }

    try:
        async with aiohttp.ClientSession() as session:
            # Run Alchemy + Etherscan concurrently
            alchemy_task = asyncio.create_task(
                _alchemy_nft_metadata(session, contract_address)
            )
            etherscan_source_task = asyncio.create_task(
                _etherscan_source(session, contract_address)
            )
            etherscan_creation_task = asyncio.create_task(
                _etherscan_creation_tx(session, contract_address)
            )

            alchemy_data, eth_source, eth_creation = await asyncio.gather(
                alchemy_task,
                etherscan_source_task,
                etherscan_creation_task,
                return_exceptions=True,
            )

            # ── Alchemy NFT API ──────────────────────────────────────────────
            if isinstance(alchemy_data, dict) and alchemy_data:
                contract_meta = alchemy_data.get("contractMetadata") or alchemy_data
                result["name"] = contract_meta.get("name") or "Unknown"
                result["symbol"] = contract_meta.get("symbol") or "Unknown"

                token_type = (contract_meta.get("tokenType") or "").upper()
                if "721" in token_type:
                    result["standard"] = "ERC-721"
                elif "1155" in token_type:
                    result["standard"] = "ERC-1155"

                total = contract_meta.get("totalSupply")
                if total is not None:
                    try:
                        result["total_minted"] = int(total)
                    except (ValueError, TypeError):
                        pass

            # ── On-chain fallback for name/symbol ───────────────────────────
            if result["name"] == "Unknown":
                name = await _eth_call_string(w3, contract_address, _NAME_SIG)
                if name:
                    result["name"] = name
            if result["symbol"] == "Unknown":
                symbol = await _eth_call_string(w3, contract_address, _SYMBOL_SIG)
                if symbol:
                    result["symbol"] = symbol

            # ── Etherscan source (verification) ─────────────────────────────
            if isinstance(eth_source, dict) and eth_source:
                source_code = eth_source.get("SourceCode", "")
                result["verified"] = bool(source_code and source_code != "")

            # ── Etherscan creation (deployer + deploy time) ──────────────────
            if isinstance(eth_creation, dict) and eth_creation:
                result["deployer"] = eth_creation.get("contractCreator")
                tx_hash = eth_creation.get("txHash")
                if tx_hash:
                    deployed_at = await _etherscan_block_time(session, tx_hash)
                    result["deployed_at"] = deployed_at
                    result["contract_age_str"] = _age_str(deployed_at)

    except Exception as exc:
        logger.error("Enrichment outer error for %s: %s", contract_address, exc, exc_info=True)

    # ── Watched wallet check (uses live in-memory list) ─────────────────────
    try:
        normalised_minters = {m.lower() for m in minters}
        normalised_watched = {w.lower() for w in config.WATCHED_WALLETS}
        hits = normalised_minters & normalised_watched
        if hits:
            result["watched_wallet_hit"] = True
            result["watched_wallet_addresses"] = [
                next(m for m in minters if m.lower() == h) for h in hits
            ]
    except Exception as exc:
        logger.warning("Watched wallet check error: %s", exc)

    logger.info(
        "Enrichment complete for %s — name=%s standard=%s verified=%s",
        contract_address,
        result["name"],
        result["standard"],
        result["verified"],
    )
    return result
