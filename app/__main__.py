"""Entry point with crash logging."""
import asyncio
import logging
import sys
import traceback

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("aks_brain")


def main():
    log.info("=== AKS Knowledge Brain starting ===")
    try:
        from app.bot import main as bot_main
        asyncio.run(bot_main())
    except KeyboardInterrupt:
        log.info("Shutting down (keyboard interrupt)")
    except Exception:
        log.critical("FATAL ERROR:\n%s", traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
