"""Backward-compatible wrapper for migration CLI."""

from .migration.cli import build_parser, main, positive_int

__all__ = ["build_parser", "main", "positive_int"]


if __name__ == "__main__":
    raise SystemExit(main())
