"""
main.py — Aster Mint Detector
Entry point. Runs the detector and Telegram bot concurrently via asyncio.gather.
All secrets are loaded from .env by config.py on import.
"""

import asyncio
import logging
import sys

# Configure structured logging to stdout before importing modules
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

import detector  # noqa: E402 — after logging setup
import alerter   # noqa: E402 — after logging setup


async def main() -> None:
    logger.info("Aster Mint Detector starting up...")
    _detector = detector.Detector()
    await asyncio.gather(
        _detector.run(),
        alerter.run_bot(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Aster Mint Detector stopped by operator.")
