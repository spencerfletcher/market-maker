"""Shared WS-feed health helpers used by both venue feeds (poly_us + kalshi).

Kept venue-neutral and dependency-free so either feed can import it without coupling to the
other's package.
"""
from __future__ import annotations


def _stale_book_reconnect(now: float, last_book_change_ts: float, n_subscribed: int,
                          last_forced_ts: float, threshold: float, cooldown: float,
                          books_should_move: bool = True) -> bool:
    """The DATA-freshness / zombie watchdog (separate from CONNECTION liveness, which ping/pong
    owns). True iff the socket should force-reconnect because no subscribed market's tradeable
    top-of-book has changed for `threshold`s — catching BOTH 'no frames at all' AND the
    frozen-resend zombie (frames arriving, book unchanged), since `last_book_change_ts` advances
    only on a REAL change (see each feed's change detector: poly_us `_note_book` /
    kalshi `_note_price_change`). Gated by ≥1 subscribed market (nothing to expect otherwise) and a
    `cooldown` since the last forced reconnect (anti-storm). Pure — no I/O, no clock read.

    `books_should_move` (default True) is the active-game-window gate: when False — no game is in
    its window, so a quiet book is EXPECTED, not a freeze — the watchdog stays its hand. Default
    True keeps the freeze check BYTE-IDENTICAL for callers that don't pass it (the Kalshi feed) and
    for the Poly feed during a live game. It only ADDS an off-hours skip; it never weakens
    real-freeze detection while a game is live (the caller passes True then)."""
    if n_subscribed <= 0:
        return False
    if not books_should_move:
        return False
    if now - last_book_change_ts <= threshold:
        return False
    return now - last_forced_ts >= cooldown
