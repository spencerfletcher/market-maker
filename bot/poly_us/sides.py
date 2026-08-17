"""
bot/poly_us/sides.py
────────────────────
Side encoding for Poly US two-outcome (moneyline) markets.

A moneyline game is a SINGLE binary slug: buying the long side takes one team,
buying the short side takes the other. Both legs share the one slug, so we encode
the short side as "<slug>::short". The scanner emits these tokens into
MarketPair.token_*; the feed prices them (long = bestAsk, short = 1 − bestBid),
and the client routes them (long = BUY_LONG, short = BUY_SHORT). This module is
the single source of truth for the suffix.

Soccer (drawable) slugs never carry the suffix → they always parse as long, so the
existing World Cup path is unaffected.
"""
from __future__ import annotations

_SHORT_SUFFIX = "::short"


def short_token(slug: str) -> str:
    """Return the short-side token for a market slug."""
    return f"{slug}{_SHORT_SUFFIX}"


def parse_token(token: str) -> tuple[str, bool]:
    """Split a Poly US token into (bare_slug, is_short)."""
    if token.endswith(_SHORT_SUFFIX):
        return token[: -len(_SHORT_SUFFIX)], True
    return token, False
