"""Venue timestamp parsing, shared by bot/ and scripts/.

Moved from `scripts/capital_ledger._iso_to_ts` [belief-recovery v5 §5 / r4 N2]: the maker's
recovery walk needs the SAME parse the ledger uses (nanosecond ISO; a naive parse shifts 4h on
this box), and bot/ importing scripts/ would invert the layering. `capital_ledger` re-exports
this under its old name, so existing importers and tests are untouched.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone

_FRACTION = re.compile(r"\.\d+")


def iso_to_ts(s) -> float | None:
    """Venue timestamps carry NANOseconds (`...:48.518028365Z`). The fraction is truncated to
    microseconds explicitly — portability insurance, not a live fix: the pinned interpreter
    here accepts any fraction length, so this line is unkillable by mutation on this box and
    the test pins the resulting EPOCH rather than the mechanism.

    ⚠️ A NAIVE timestamp (no zone) is read as **UTC**, not local. Both venues stamp UTC; letting
    `datetime.timestamp()` apply the box's local zone would shift every such row by the UTC
    offset, silently, and a tax ledger is exactly where a whole-day shift matters."""
    if not isinstance(s, str) or not s:
        return None
    txt = s.strip()
    if txt.endswith(("Z", "z")):
        txt = txt[:-1] + "+00:00"
    m = _FRACTION.search(txt)
    if m and len(m.group(0)) > 7:                    # "." + more than 6 digits
        txt = txt[:m.start()] + m.group(0)[:7] + txt[m.end():]
    try:
        dt = datetime.fromisoformat(txt)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def ts_to_iso(ts: float) -> str:
    """Epoch seconds → `2026-08-26T12:34:56Z`. ⛔ WITH THE YEAR: the trip/episode banks span
    months and will span years, and a `08-26T…` stamp pasted into a decision note is a date
    that cannot be looked up. Sub-second is truncated, never rounded."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))


def stamp_to_ts(raw: object) -> float | None:
    """A tape stamp as epoch seconds — **epoch OR ISO**, `None` when neither parses.

    ⛔ Tapes disagree on the form by COLUMN, not by file: `probe_verdicts.written_ts` is ISO
    (`poly_probe_night` writes `time.strftime("%Y-%m-%dT%H:%M:%SZ")`), while `picks.pick_ts` and
    `fills.ts` are epoch. A reader that tries only one form drops every row of the other silently
    — which is exactly what a `--since` filter must never do.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return iso_to_ts(text)
