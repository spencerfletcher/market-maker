"""Venue timestamp parsing, shared by bot/ and scripts/.

Lifted out of the ledger tooling so it can be shared: the maker's recovery walk needs the SAME
parse the ledger uses (nanosecond ISO; a naive parse shifts by the local UTC offset), and having
`bot/` import from `scripts/` would invert the layering. The ledger re-exports this under its old
name, so existing importers and tests are untouched.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

_FRACTION = re.compile(r"\.\d+")


def iso_to_ts(s) -> float | None:
    """Venue timestamps carry NANOseconds (`...:48.518028365Z`). The fraction is truncated to
    microseconds explicitly — portability insurance, not a live fix: the pinned interpreter
    accepts any fraction length, so mutating this line changes no observable behaviour here and
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
