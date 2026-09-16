"""bot/core/net_positions.py

THE VENUE POSITION READ and the resting-queue-at-a-price read — two helpers six tools share.

WHY THIS EXISTS. Both were private functions inside `scripts/poly_rebate_probe.py`, and four
position tools plus two book tools imported `_net_positions` / `_tob_at` THROUGH the probe — so
asking the venue "what am I holding" ran an armed real-money probe's module body, its argparse
tree and its CSV path constants. Cut out on 2026-09-04; the probe imports them back and its
behaviour is unchanged.

⛔ NEITHER FUNCTION PLACES, CANCELS OR MUTATES ANYTHING. `_net_positions` is a READ of the venue's
own position list; `_tob_at` is a read of a book frame already in hand. Decimal throughout, parsed
from the venue's string form via `bot.core.money.parse_wire`.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Callable, Optional

from bot.core.money import is_zero, parse_wire

# ⛔ THE VENUE CLIENT IS NOT IMPORTED AT MODULE LEVEL. `bot.poly_us.client` pulls `bot.core.config`,
# which calls `load_dotenv()` and READS `.env` on import — so a module-level import here would make
# "what am I holding" un-importable without the venue's environment, which is the coupling this
# module exists to remove. The annotation is lazy (`from __future__ import annotations`) and
# `_level_px` is imported at its one call site.
if TYPE_CHECKING:
    from bot.poly_us.client import PolyUSClient

def _tob_at(md: dict | None, key: str, price: Decimal | None) -> Decimal | None:
    """Total qty resting AT `price` on one side (`key` = "bids" or "offers") of a book.

    LOGGING ONLY — the `tob_ahead` CSV column, i.e. the queue a JOINING quote sits behind. Nothing
    gates on it. `touch_from_md` already returns this for the BID side and stays the parser of
    record for the touch PRICES, but it does not expose the ask side, and a sell run that logged
    the bid queue would be recording the wrong side of the book in the one column that says how
    many contracts were in front of us. On the real target books the two sides differ by orders of
    magnitude (usse-dem: 1,110 bid vs 251,965 ask), so this is not a rounding difference.

    Same rule as its sibling: SUM at the best price, never level 0's slice. None when the side is
    empty or the price is unknown; never raises (it rides the same read as the touch)."""
    if price is None or not isinstance(md, dict):
        return None
    from bot.poly_us.client import _level_px          # deferred: see the module header
    try:
        levels = [lv for lv in (md.get(key) or []) if isinstance(lv, dict)]
        return sum((parse_wire(lv.get("qty", "0")) for lv in levels if _level_px(lv) == price),
                   Decimal(0))
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None


async def _net_positions(client: PolyUSClient,
                         on_error: Optional[Callable[[BaseException], None]] = None,
                         ) -> dict[str, Decimal] | None:
    """Every open Poly position as a SIGNED net quantity, or None = CANNOT VERIFY.

    `on_error` is an OPTIONAL observer of the transport exception this reader swallows on its
    way to `None` [2026-08-25]. It changes nothing: the return value, the printed line and
    every existing caller are byte-identical without it. It exists because `None` alone cannot
    distinguish a venue 503 from a Cloudflare 1015 rate-limit BAN, and a poller that keeps its
    cadence through a 1015 extends the ban — the defect that cost the 2026-08-25 evening
    relaunch (`scripts.poly_pos_fleet.is_rate_limited`). The callback must not raise; it is
    called inside the read's own error path.

    ⚠️ **Why not `scripts.poly_postonly_probe._positions`, which this file used until 2026-07-28.**
    That reader goes through `reconcile._first_qty`, which takes `abs()` — it answers "how much",
    never "which way". For the ask-side run the SIGN *is* the experiment: a resting BUY_SHORT that
    fills must leave `netPosition` NEGATIVE on the long slug, and a POSITIVE reading would mean the
    intent mapping is backwards and we opened a second long rather than a short. A magnitude
    standing in for a sign would pass that check while it was wrong, on the one run whose purpose
    is to make it. Everything else is deliberately the same code: paginate to `eof`, `None` and
    `{}` are different answers, dust is zero at the venue's resolution.

    `netPositionDecimal` ONLY [box probe 2026-08-06: the venue's EXACT holding beside the
    rounded `netPosition` display] — no `qtyAvailable` fallback (sign convention never
    verified; a fallback that silently answers the sign question from an unverified field
    is the failure this docstring is about) and no rounded-field fallback (a half-contract
    lie feeding the guards that import this reader). A position missing the exact field
    reads as CANNOT VERIFY.

    Parsed with `parse_wire` from the venue's raw STRING (`netPosition: str`, SDK
    types/portfolio.py) — never via float (CLAUDE.md § Code style)."""
    out: dict[str, Decimal] = {}
    cursor, pages = "", 0
    while pages < 50:
        params: dict = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            resp = await client._sdk.portfolio.positions(params)
        except Exception as e:
            print(f"    positions read FAILED: {e!r}")
            if on_error is not None:
                on_error(e)
            return None
        if not isinstance(resp, dict):
            print(f"    positions shape unexpected ({type(resp).__name__}) — CANNOT VERIFY")
            return None
        positions = resp.get("positions")
        if not isinstance(positions, dict):
            print(f"    `positions` not a dict ({type(positions).__name__}) — CANNOT VERIFY")
            return None
        for slug, item in positions.items():
            # ⛔ `netPositionDecimal`, REQUIRED [box probe 2026-08-06 — the P1 answer]:
            # the venue HOLDS fractionally and reports it exactly in this field, while
            # `netPosition` is a ROUNDED display ("-11" beside "-10.6000", observed live).
            # No fallback to the rounded field — a silent half-contract lie here feeds
            # the guards (pos_guard/fill_watch import this reader) and reduce-only.
            raw = item.get("netPositionDecimal") if isinstance(item, dict) else None
            if raw is None or raw == "":
                print(f"    position {slug}={item!r} has no netPositionDecimal — "
                      f"CANNOT VERIFY (the rounded netPosition is not a substitute)")
                return None
            try:
                qty = parse_wire(raw)
            except (TypeError, ValueError, ArithmeticError):
                print(f"    unparseable netPositionDecimal {slug}={raw!r} — CANNOT VERIFY")
                return None
            if not qty.is_finite():
                print(f"    non-finite netPositionDecimal {slug}={raw!r} — CANNOT VERIFY")
                return None
            if not is_zero(qty):        # dust-safe; sign PRESERVED
                out[str(slug)] = qty
        cursor, pages = resp.get("nextCursor") or "", pages + 1
        if resp.get("eof", True) or not cursor:
            return out
    return None
