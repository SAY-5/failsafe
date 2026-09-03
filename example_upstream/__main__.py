from __future__ import annotations

import argparse

import uvicorn


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="example_upstream")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args(argv)
    uvicorn.run("example_upstream.app:app", host=args.host, port=args.port, access_log=False)


if __name__ == "__main__":
    main()
