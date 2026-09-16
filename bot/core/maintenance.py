"""The venue's RESERVED weekly maintenance window: the constants, and the ONE pure predicate.

⛔ IT LIVES IN `bot/core` BECAUSE BOTH SIDES ASK. `scripts.poly_launch` classifies a whole run's
overlap with it (`maintenance_overlap`, which imports these names) and `bot/poly_us/maker.py`
asks, per cycle, "am I inside it right now" (`maintenance_guard_active`, the 503-wall hold). A
`scripts.*` import from the maker is an import cycle, and a second copy of the constants is how
the window moves in one file only.

The window itself, its provenance and its consequences: `scripts/poly_launch.py` § the venue's
maintenance window, and the private design notes § Maintenance posture.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

#: `datetime.weekday()` — 0 = Monday, so 3 = Thursday. ⛔ NOT NIGHTLY: this gates everything below.
MAINT_WEEKDAY = 3
MAINT_START_LOCAL = "02:00"
MAINT_END_LOCAL = "06:00"
#: ⛔ A ZONE NAME, NEVER A UTC OFFSET — 06:00-10:00Z under EDT, 07:00-11:00Z under EST.
MAINT_TZ = "America/New_York"


def maint_hhmm(text: str) -> tuple[int, int]:
    h, m = text.split(":")
    return int(h), int(m)


def maintenance_window_for(ts: float) -> Optional[tuple[float, float]]:
    """The window `ts` falls inside, as `(open_epoch, close_epoch)`, or None.

    ⛔ PURE — the clock is an argument, never `time.time()`.
    ⛔ CLOSED INTERVAL, like `poly_launch.maintenance_overlap`: an instant exactly on either edge
    counts as INSIDE, which is the conservative direction for every caller (a defensive posture
    held one instant too long costs nothing; released one instant too early is the exposure).
    The window does not cross local midnight, so `ts`'s own local date carries it.
    """
    zone = ZoneInfo(MAINT_TZ)
    local = datetime.fromtimestamp(ts, zone)
    if local.weekday() != MAINT_WEEKDAY:
        return None
    sh, sm = maint_hhmm(MAINT_START_LOCAL)
    eh, em = maint_hhmm(MAINT_END_LOCAL)
    day = local.date()
    # ⚠️ `fold=1` on the START, as in `poly_launch._maintenance_windows`: on the spring-forward
    # date local 02:00 does not exist, and fold=1 yields the WIDER window — guarding MORE.
    w0 = datetime(day.year, day.month, day.day, sh, sm, tzinfo=zone, fold=1).timestamp()
    w1 = datetime(day.year, day.month, day.day, eh, em, tzinfo=zone).timestamp()
    return (w0, w1) if w0 <= ts <= w1 else None


def in_maintenance_window(ts: float) -> bool:
    """Is `ts` inside the venue's maintenance window? See `maintenance_window_for`."""
    return maintenance_window_for(ts) is not None
