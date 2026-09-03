"""Command line entrypoint: `python -m failsafe` or `failsafe`."""

from __future__ import annotations

import argparse
import logging

import uvicorn

from failsafe.app import create_app
from failsafe.config import load


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="failsafe", description="Resilient API gateway")
    parser.add_argument("--config", help="path to routes.yaml (default: $FAILSAFE_CONFIG)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--access-log", action="store_true", help="enable per-request logs")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    app = create_app(load(args.config))
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=args.access_log,
        proxy_headers=False,
    )


if __name__ == "__main__":
    main()
