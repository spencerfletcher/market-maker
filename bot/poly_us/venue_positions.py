"""
bot/poly_us/venue_positions.py
──────────────────────────────
The paginating, fail-closed venue positions read, moved VERBATIM out of `scripts/poly_live_mm.py`
[2026-09-15] so `PolyMaker._periodic_venue_verify` imports it from inside `bot/`. The launcher
re-imports every name from here — including `_VERIFY_REFUSALS`, the SAME list object its
`_drain_verify_refusals` drains into the teardown certification note.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from bot.poly_us.client import PolyUSClient


#: Refused verify reads, by exception CLASS, drained into the teardown certification's `reads`
#: (and therefore into its note). A list, not a flag: two refused reads are two lines of evidence.
_VERIFY_REFUSALS: list[str] = []


async def _verify_positions(client: PolyUSClient, *,
                            record: bool = True) -> dict[str, tuple[str, str]] | None:
    """`_venue_positions` for the TEARDOWN's verify block, where a raise cannot be allowed.

    ⛔ `record=False` FOR ANY CALLER THAT IS NOT THE TEARDOWN:
    `_VERIFY_REFUSALS` is drained into the teardown CERTIFICATION note, so the maker's hourly
    verify (`PolyMaker._periodic_venue_verify`) would file a transient 03:00 read refusal as a
    finding about a teardown that ran hours later. The refusal is still printed and still returns
    `None` — only the durable certification evidence is scoped to the phase that owns it.

    ⛔ That block runs inside the teardown `finally`, so anything escaping it skips
    `teardown_cert.record` and leaves NO row — and absence is the one verdict an operator may
    attest away, while a recorded refusal cannot be. A refusal from
    the shared request budget (`VenueBanned` while the box IP is rate-limited) is exactly that
    shape, and it is a CANNOT-VERIFY like any other unreadable venue: `None`, never `{}`.
    """
    try:
        return await _venue_positions(client)
    except Exception as exc:
        # ⛔ THE CLASS NAME GOES INTO THE CERTIFICATION NOTE, not just the log. `VenueBanned` (the
        # box's own rate-limit flag, inspectable and deletable) and a venue timeout demand
        # different diagnosis, and the row is the only thing that crosses the process boundary.
        if record:
            _VERIFY_REFUSALS.append(f"read REFUSED: {type(exc).__name__}")
        print(f"  positions read REFUSED: {exc!r} — CANNOT VERIFY", flush=True)
        return None



async def _venue_positions(client: PolyUSClient) -> dict[str, tuple[str, str]] | None:
    """Every open Poly position, or None = CANNOT VERIFY.

    ⛔ None and {} must never be confused: {} is "confirmed flat" and may start, None is "we do not
    know" and must refuse — collapsing the two is how `bot/runner/reconcile.py` reported "confirmed
    flat" for a month while we held positions. Paginates to the end, because stopping at page one
    reproduces the same bug in a different shape.
    """
    out: dict[str, str] = {}
    cursor, pages = "", 0
    while pages < 50:
        params: dict = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            # ⛔ The continuation key below is `nextCursor`, NOT `cursor` — reading the wrong key
            # made this loop always break after page one while the docstring above claimed it
            # paginates to the end. Fail direction was OPEN: unseen positions read as absent,
            # i.e. flat. Same key `bot/runner/reconcile.py` uses. [mm-review 2026-07-28 round 2]
            resp = await client._sdk.portfolio.positions(params)
        except Exception as exc:
            print(f"  positions read FAILED: {exc!r} — CANNOT VERIFY", flush=True)
            return None
        if not isinstance(resp, dict):
            print(f"  positions shape unexpected ({type(resp).__name__}) — CANNOT VERIFY")
            return None
        positions = resp.get("positions")
        if not isinstance(positions, dict):
            print(f"  `positions` not a dict ({type(positions).__name__}) — CANNOT VERIFY")
            return None
        for slug, item in positions.items():
            if not isinstance(item, dict):
                print(f"  position for {slug} is {type(item).__name__}, not an object — "
                      f"CANNOT VERIFY")
                return None
            # `dict.get(k, default)` does NOT consult the default when the key is PRESENT AND
            # NULL — check presence explicitly; an unreadable position is cannot-verify, never
            # absent. ⛔ `netPositionDecimal` REQUIRED: the EXACT holding ("-10.6000") beside the
            # ROUNDED display `netPosition` ("-11"). No fallback: missing = CANNOT VERIFY.
            raw = item.get("netPositionDecimal")
            if isinstance(raw, dict):
                raw = raw.get("value")
            if raw is None:
                print(f"  position for {slug} has no netPositionDecimal — CANNOT VERIFY "
                      f"(the rounded netPosition is not a substitute; refusing to read "
                      f"it as flat)")
                return None
            # `updateTime` rides along as evidence for the operator: the endpoint serves divergent
            # replicas, and when reads disagree the venue's own row timestamps are what a human
            # reconciles with.
            out[str(slug)] = (str(raw), str(item.get("updateTime") or "?"))
        cursor = resp.get("nextCursor") or ""
        pages += 1
        if not cursor or resp.get("eof"):
            break
    else:
        # Ran out of pages with a cursor still live: we have NOT seen every position.
        print("  positions pagination hit the 50-page ceiling with more to read — CANNOT VERIFY")
        return None
    return out



def venue_qty(row: Any) -> Decimal:
    """The signed quantity out of ONE `_venue_positions` row.

    ⛔ A ROW IS `(qty_string, updateTime)`, NOT a quantity — `Decimal(str(row))` on the tuple
    raises `InvalidOperation`, which is how a consumer that assumed the flat shape took its whole
    caller down. `teardown_cert.decide_residual` carries
    the same unpack; every new reader comes through here instead of spelling it a third time.
    """
    return Decimal(str(row[0] if isinstance(row, (tuple, list)) else row))
