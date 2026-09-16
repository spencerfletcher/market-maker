"""Pins the live paper-MM's loss-math (`_mark_step`) — the real-money kill-switch's correctness.

The dangerous properties, each pinned below:
  • P&L is 0 at the moment of a fill and accrues ONLY on an adverse move. ⚠️ EXIT-SIDE since
    2026-07-20, not mid: a buy books at the BID and the long it creates marks at the BID, so the two
    cancel to 0 at the fill and a loss appears only when that side moves against the position.
    Booking both legs at the mid instead made a maker round trip net exactly ZERO — blind to the
    spread capture that is the maker's whole income.
  • Pre-existing positions (the `base` baseline) are excluded from BOTH cash and inventory — only OUR
    fills-from-start count.
  • A ticker with no mid this cycle is SPLIT, not skipped wholesale: its BOOKED inventory is marked
    at the settlement bound (long→0, short→1) so the cap cannot be disarmed, while the UNBOOKED
    delta is excluded and `last` is NOT advanced (so that fill re-books when the book returns).
The get_positions-outage fail-CLOSED decision lives at the call site (cur is None → halt), not here;
this module pins the arithmetic that runs once cur is known.
"""
from decimal import Decimal as D

from bot.kalshi import maker as mm
from bot.kalshi.maker import (
    _book_mid, _event_of, _maker_mult, _mark_step, _series_of,
)

T = ["A"]


# ── selection guardrails: contested-band + one-per-event (learned from auto-picking both sides of a
#    0.04/0.97 near-decided game — the worst place to measure MM economics). ──
def test_event_of_shares_key_across_complementary_sides():
    a = "KXNPBGAME-26JUL190100CHUYOM-CHU"
    b = "KXNPBGAME-26JUL190100CHUYOM-YOM"
    assert _event_of(a) == _event_of(b) == "KXNPBGAME-26JUL190100CHUYOM"


def test_book_mid_skips_a_single_level_NaN_book_instead_of_raising():
    """[round-2 review] On a SINGLE-level ladder a NaN price survives the parse (with >=2
    levels `max()` raises inside the try) and `Decimal("NaN") < x` raises InvalidOperation
    OUTSIDE it — one garbage level in one REST book aborted target selection where the float
    path skipped the book. json.loads accepts bare NaN, so this needs no quoted string."""
    assert _book_mid({"yes_dollars": [["NaN", 10]],
                      "no_dollars": [["0.58", 5]]}) is None
    assert _book_mid({"yes_dollars": [["0.40", 10]],
                      "no_dollars": [["NaN", 5]]}) is None
    assert _book_mid({"yes_dollars": [["Infinity", 10]],
                      "no_dollars": [["0.58", 5]]}) is None


def test_book_mid_contested_vs_tail():
    # contested: yes bid 0.40, no bid 0.58 → yes ask 0.42 → mid 0.41. EXACT: in float the
    # complement `1 - 0.58` is 0.42000000000000004 and the mid comes out 0.41000000000000003,
    # which is what used to sit one representation error from the contested band's edge.
    assert _book_mid({"yes_dollars": [["0.40", 10]], "no_dollars": [["0.58", 5]]}) == D("0.41")
    # near-decided tail: yes bid 0.03, no bid 0.95 → yes ask 0.05 → mid 0.04 (would be filtered out)
    assert _book_mid({"yes_dollars": [["0.03", 5]], "no_dollars": [["0.95", 5]]}) == D("0.04")


def test_book_mid_none_on_one_sided_or_crossed():
    assert _book_mid({"yes_dollars": [], "no_dollars": [[0.50, 1]]}) is None          # one-sided
    assert _book_mid({"yes_dollars": [[0.60, 1]], "no_dollars": [[0.60, 1]]}) is None  # crossed (ya<yb)
    assert _book_mid({}) is None                                                       # empty


def test_book_mid_accepts_legacy_demo_shape():
    assert _book_mid({"yes": [["0.45", 1]], "no": [["0.50", 1]]}) == D("0.475")


# ── phase classification + flow ranking (target selection) ───────────────────────────────
# The old `_live()` asked "is expiration−3h ≤ now < expiration" and called that liveness. On a market
# with no game clock (weather, macro, "LeBron's next team" 93 days out) that question is meaningless
# and silently answered False. `_phase` names the long-dated case instead of failing a liveness check.
import datetime as _dt
import time

import pytest

from bot.kalshi.maker import (
    _expiry_ts, _phase, _two_sided, eligible_tickers, feed_prefixes, wait_for_book,
)


class _M:
    def __init__(self, hours_to_expiry, ticker="T", volume_24h=0.0):
        self.ticker = ticker
        self.volume_24h = volume_24h
        self.expected_expiration_time = (
            _dt.datetime.fromtimestamp(time.time() + hours_to_expiry * 3600, _dt.timezone.utc)
            .isoformat().replace("+00:00", "Z"))


def test_phase_distinguishes_a_game_clock_from_no_game_clock():
    assert _phase(_M(1.0)) == "live"          # inside the 3h game window
    assert _phase(_M(6.0)) == "pregame"       # today, not started
    assert _phase(_M(24 * 93)) == "long-dated"  # KXNEXTTEAMNBA — the category the old test mis-answered
    assert _phase(_M(-1.0)) == "closing"


def test_a_past_expiry_market_is_flagged_closing():
    """Kalshi leaves a market `open` for hours past expected expiry (settlement lag / postponement
    buffer). A DRY preview selected one 14h expired — the result is effectively known and it can settle
    out from under our inventory, so selection must be able to see and exclude it."""
    assert _phase(_M(-14.0)) == "closing"


def test_phase_is_unknown_not_false_when_timing_is_unreadable():
    """The old code collapsed 'no timestamp' into 'not live', which is how an unparseable market became
    indistinguishable from a pre-game one."""
    class _NoTime:
        expected_expiration_time = ""
    class _Garbage:
        expected_expiration_time = "not-a-timestamp"
    assert _phase(_NoTime()) == "unknown" and _expiry_ts(_NoTime()) is None
    assert _phase(_Garbage()) == "unknown"


_MM_TARGETS = ["KXFED-26SEP-T3.75", "KXCPI-26JUL-T0.1", "KXGDP-26JUL30-T2.5",
               "KXMLBTOTAL-26JUL191610SFSEA-8"]


def test_the_default_feed_prefixes_would_drop_every_mm_target():
    """WHY the fix is needed. `feed._matches_prefix` is a hard filter on inbound WS messages and
    defaults to config.KALSHI_SERIES — the 12 cross-arb SPORTS series. None of these MM targets match
    it, so every book message was dropped, the book never populated, `_quote` returned early on
    `not bid or not ask`, and two real-money runs (2026-07-19) placed ZERO orders for their full
    duration while logging a healthy WS connect+subscribe. Note KXMLBTOTAL does NOT match KXMLBGAME."""
    from bot.core import config
    assert not any(any(t.startswith(p) for p in config.KALSHI_SERIES) for t in _MM_TARGETS)


def test_feed_prefixes_cover_targets_the_default_would_drop():
    assert feed_prefixes(_MM_TARGETS) == ["KXCPI", "KXFED", "KXGDP", "KXMLBTOTAL"]
    from bot.kalshi.feed import KalshiOrderBookCache
    cache = KalshiOrderBookCache(object(), series_prefixes=feed_prefixes(_MM_TARGETS))
    # exercise the REAL filter, not a re-derivation of the expression under test
    assert all(cache._matches_prefix(t) for t in _MM_TARGETS)


def test_the_cache_is_never_constructed_without_explicit_prefixes():
    """Pins the CALL SITE, not the expression. The first version of this test re-derived
    `sorted({_series_of(t) ...})` in its own body and asserted on that — so deleting `series_prefixes=`
    from the constructor left it green: a test named for a regression it could not detect. The
    constructor silently falls back to config.KALSHI_SERIES, so omitting the kwarg is the whole bug."""
    import inspect

    from bot.kalshi import maker as _mm
    src = inspect.getsource(_mm)
    calls = [ln for ln in src.splitlines() if "KalshiOrderBookCache(" in ln and "import" not in ln]
    assert calls, "constructor call not found — did it move or get renamed?"
    for ln in calls:
        assert "series_prefixes=" in ln, f"cache built with the default 12-series filter: {ln.strip()}"


@pytest.mark.asyncio
async def test_wait_for_book_refuses_when_no_target_ever_populates():
    """The refusal branch: this is what turns a silent 90-minute no-op into an immediate abort."""
    class _Dead:
        def get_best_bid_d(self, t): return None
        def get_best_ask_d(self, t): return None

    assert await wait_for_book(_Dead(), _MM_TARGETS, tries=3, delay=0) is False


@pytest.mark.asyncio
async def test_wait_for_book_accepts_ANY_two_sided_target():
    """ANY, not ALL: a genuinely one-sided quiet market must not abort the run, while the failure this
    guards (a filter dropping everything) takes out every target at once."""
    class _OneGood:
        def get_best_bid_d(self, t): return D("0.44") if t.startswith("KXGDP") else None
        def get_best_ask_d(self, t): return D("0.45") if t.startswith("KXGDP") else None

    assert await wait_for_book(_OneGood(), _MM_TARGETS, tries=3, delay=0) is True


@pytest.mark.asyncio
async def test_a_one_sided_book_does_not_count_as_populated():
    """A bid with no ask is not quotable — `_quote` returns early on it — so it must not satisfy the
    gate either, or the gate would pass while the run still places nothing."""
    class _HalfBook:
        def get_best_bid_d(self, t): return D("0.44")
        def get_best_ask_d(self, t): return None

    assert await wait_for_book(_HalfBook(), _MM_TARGETS, tries=2, delay=0) is False


def test_eligibility_excludes_expired_and_admits_the_rest():
    ms = [_M(-14.0, "expired"), _M(1.0, "live"), _M(6.0, "pregame"), _M(24 * 93, "longdated")]
    assert eligible_tickers(ms) == ["live", "pregame", "longdated"]
    assert eligible_tickers(ms, pregame_only=True) == ["pregame", "longdated"]


def test_unreadable_timing_is_INELIGIBLE_not_eligible():
    """The one fail-OPEN branch this gate must not have. If `expected_expiration_time` ever goes missing
    or changes shape, EVERY market classifies `unknown`; admitting those would make both the expiry
    exclusion and --pregame-only silently inert while still reading as armed, re-admitting the 14h-
    expired market the gate exists to exclude. Cannot-verify is not permission."""
    class _NoTime:
        ticker = "u"
        expected_expiration_time = ""
    assert eligible_tickers([_NoTime()]) == []
    assert eligible_tickers([_NoTime()], pregame_only=True) == []


@pytest.mark.asyncio
async def test_selection_ranks_by_flow_within_a_fee_class(monkeypatch):
    """The regression this exists to prevent: the picker took whatever the scanner listed FIRST, which
    put a throughput run on KXNPBGAME (1,883 contracts/24h) instead of KXMLBTOTAL (2,686,059)."""
    monkeypatch.setattr(mm, "_MAKER_CHARGED", {"KXNPBGAME": False, "KXMLBTOTAL": False}, raising=False)

    class _C:
        async def get_orderbook(self, t):
            return {"orderbook_fp": {"yes_dollars": [[0.45, 10]], "no_dollars": [[0.52, 10]]}}

    thin, thick = "KXNPBGAME-e1-A", "KXMLBTOTAL-e2-B"
    got = await _two_sided(_C(), [thin, thick], 1, vol_of={thin: 1_883, thick: 2_686_059})
    assert got == [thick]


@pytest.mark.asyncio
async def test_maker_fee_still_outranks_flow(monkeypatch):
    """Fee is the FIRST sort key: a huge-flow charged series must not outrank a maker-free one, since
    the ~0.44c/fill fee roughly cancels the whole measured markout edge."""
    monkeypatch.setattr(mm, "_MAKER_CHARGED", {"KXMLBGAME": True, "KXNPBGAME": False}, raising=False)

    class _C:
        async def get_orderbook(self, t):
            return {"orderbook_fp": {"yes_dollars": [[0.45, 10]], "no_dollars": [[0.52, 10]]}}

    charged, free = "KXMLBGAME-e1-A", "KXNPBGAME-e2-B"
    got = await _two_sided(_C(), [charged, free], 1, vol_of={charged: 9_000_000, free: 10})
    assert got == [free]


@pytest.mark.asyncio
async def test_the_contested_band_is_INCLUSIVE_AND_SYMMETRIC_at_both_edges(monkeypatch):
    """`--min-price` / `--max-price` are argparse FLOATS and the mid is exact Decimal, so the gate
    compares across a boundary. It has to cross `from_float` first, or the band is silently LOPSIDED:
    `float(0.15)` sits just BELOW 0.15 and `float(0.85)` just BELOW 0.85 — the same direction, which
    on one side of the band is the wrong one. A book whose exact mid is 0.15 was therefore admitted
    and a book whose exact mid is 0.85 was REJECTED, from a gate written `min <= mid <= max`.

    At the DEFAULT band only the UPPER edge distinguishes raw-vs-converted (`float(0.15)` sits
    below 0.15, so a raw lower bound still admits an exact 0.15 — a raw-min mutant is GREEN on
    the default cases) [round-2 review]. The `--min-price 0.1` probe below is what pins the
    LOWER conversion: `float(0.1)` sits ABOVE 0.1, so a raw min rejects an exact 0.10 mid from
    a gate written `min <= mid`. Measured over all 4,950 two-sided cent books, the raw compare
    rejected 10 real markets at the 0.85 edge that the old all-float path admitted — see
    the private design notes §4.3."""
    monkeypatch.setattr(mm, "_MAKER_CHARGED", {"KXNPBGAME": False}, raising=False)

    def _client_at(yes_bid: str, no_bid: str):
        class _C:
            async def get_orderbook(self, t):
                return {"orderbook_fp": {"yes_dollars": [[yes_bid, 10]],
                                         "no_dollars": [[no_bid, 10]]}}
        return _C()

    # yes bid 0.14, yes ask 1 − 0.84 = 0.16  ⇒  mid EXACTLY 0.15, the lower edge.
    low = "KXNPBGAME-e1-A"
    assert await _two_sided(_client_at("0.14", "0.84"), [low], 1) == [low], \
        "a mid of exactly --min-price is INSIDE an inclusive band"

    # yes bid 0.84, yes ask 1 − 0.14 = 0.86  ⇒  mid EXACTLY 0.85, the upper edge.
    high = "KXNPBGAME-e2-B"
    assert await _two_sided(_client_at("0.84", "0.14"), [high], 1) == [high], \
        "and so is a mid of exactly --max-price — the band must not be lopsided"

    # The control: one tick OUTSIDE each edge is still excluded, so the test above cannot be
    # satisfied by a gate mutated to admit everything.
    assert await _two_sided(_client_at("0.13", "0.85"), [low], 1) == []    # mid 0.14
    assert await _two_sided(_client_at("0.85", "0.13"), [high], 1) == []   # mid 0.86

    # THE LOWER-BOUND PIN [round-2 review]: float(0.1) > Decimal("0.1"), so a raw min rejects
    # an exact 0.10 mid. yes bid 0.09, yes ask 1 − 0.89 = 0.11 ⇒ mid EXACTLY 0.10.
    edge = "KXNPBGAME-e3-C"
    assert await _two_sided(_client_at("0.09", "0.89"), [edge], 1,
                            min_price=0.1) == [edge], \
        "an exact mid of --min-price 0.1 is INSIDE — a raw lower bound rejects it"


# ── maker-fee awareness. ⚠️ The fee status is read from the VENUE (/series.fee_type), never
#    inferred from the fee-schedule PDF: the PDF lists 76 maker-charged series, the API lists 130.
#    Inferring "free" from PDF absence mislabelled KXNBAGAME and KXNHLGAME as maker-free, and
#    --maker-free-only would have selected them and paid the fee it exists to avoid. ──
def test_series_of_strips_to_the_series_ticker():
    assert _series_of("KXNPBGAME-26JUL190100CHUYOM-CHU") == "KXNPBGAME"
    assert _series_of("KXMLBGAME-26JUL212140CINSEA-CIN") == "KXMLBGAME"


def test_unverified_series_is_assumed_CHARGED(monkeypatch):
    """Fail-safe default. --maker-free-only must never select a market whose fee status we could not
    read; guessing 'free' is the expensive direction."""
    monkeypatch.setattr(mm, "_MAKER_CHARGED", {}, raising=False)
    assert _maker_mult("KXANYTHING-x-y") == 1


def test_maker_mult_reflects_the_venue_not_a_hardcoded_list(monkeypatch):
    monkeypatch.setattr(mm, "_MAKER_CHARGED",
                        {"KXNPBGAME": False, "KXMLBGAME": True, "KXNBAGAME": True}, raising=False)
    assert _maker_mult("KXNPBGAME-x-y") == 0     # verified free
    assert _maker_mult("KXMLBGAME-x-y") == 1     # verified charged
    assert _maker_mult("KXNBAGAME-x-y") == 1     # the PDF-absence trap: API says CHARGED


def test_the_regression_this_exists_to_prevent(monkeypatch):
    """KXNBAGAME/KXNHLGAME are absent from the fee PDF but are `quadratic_with_maker_fees` in the
    API [VERIFIED 2026-07-19]. Under the old hardcoded set they returned 0 (free)."""
    monkeypatch.setattr(mm, "_MAKER_CHARGED", {"KXNBAGAME": True, "KXNHLGAME": True}, raising=False)
    for t in ("KXNBAGAME-x-y", "KXNHLGAME-x-y"):
        assert _maker_mult(t) == 1, t


def test_pnl_is_zero_at_the_fill_then_loss_on_adverse_move():
    base = {"A": 0.0}
    # buy 1 @ mid 0.50 → cash -0.50, inv +1, P&L 0
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, {"A": (0.50, 0.50)}, 0.0)
    assert inv["A"] == 1.0 and round(cash, 4) == -0.50 and round(pnl, 4) == 0.0
    assert fills == [("A", 1.0, 0.50)]
    # price drops to 0.40, no new fill → P&L = -0.10 (adverse), cash unchanged
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {"A": 1.0}, {"A": (0.40, 0.40)}, cash)
    assert fills2 == [] and round(cash2, 4) == -0.50 and round(pnl2, 4) == -0.10


def test_sell_fill_is_symmetric():
    base = {"A": 0.0}
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 0.0}, {"A": -1.0}, {"A": (0.50, 0.50)}, 0.0)
    assert inv["A"] == -1.0 and round(cash, 4) == 0.50 and round(pnl, 4) == 0.0


def test_baseline_excludes_pre_existing_position():
    base = {"A": 2.0}                                  # account already held 2 before we started
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 2.0}, {"A": 2.0}, {"A": (0.50, 0.50)}, 0.0)
    assert inv["A"] == 0.0 and fills == [] and round(pnl, 4) == 0.0   # pre-existing 2 does not count


def test_mid_none_skips_and_does_not_advance_last():
    base = {"A": 0.0}
    # a fill happened (0→1) but the book has no mid yet → skip: no cash, last NOT advanced
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, {"A": (None, None)}, 0.0)
    assert fills == [] and cash == 0.0 and last.get("A", 0.0) == 0.0   # last stays 0 → re-detects next cycle
    assert inv["A"] == 1.0             # position IS known even without a mid → inv always populated (no KeyError)
    # next cycle with a mid → the fill books now
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {"A": 1.0}, {"A": (0.50, 0.50)}, cash)
    assert fills2 == [("A", 1.0, 0.50)] and round(cash2, 4) == -0.50 and last2["A"] == 1.0


# ── M1: HELD inventory that cannot be marked is valued at its WORST CASE, never skipped ──────────
# The defect this pins: `_mark_step` used to skip a ticker whose mid was None, so the position's
# CASH leg stayed in the total while its MARK vanished. For a LONG that reads as a total loss (the
# cap fires spuriously); for a SHORT it reads as a total WIN — which DISARMS the kill switch at
# exactly the moment the position became untradeable. A one-sided book is common and transient, so
# halting on it would make the tool unusable in the thin markets it targets; marking at the worst
# case keeps the run alive while making the cap err safe.
#
# Worst case is the settlement bound, not a guess: a long is worth 0 if it settles NO, and a short
# YES (= long NO) is worth 1 against us if it settles YES.

def test_an_unmarkable_LONG_is_valued_at_zero_not_skipped():
    """⚠️ THIS TEST DOES NOT PIN THE SKIP, and its name overstates it. It passes with the original
    whole-ticker skip fully restored, because skipping a long coincidentally equals its worst case
    (cash already carries the full cost). What it does kill: a sign-swapped bound (long→1, short→0)
    and a mid-fallback (marking at 0.5). It pins the DIRECTION of the long bound, nothing more —
    `test_an_unmarkable_SHORT_does_NOT_read_as_a_win` is the one that pins the defect."""
    base = {"A": 0.0}
    # buy 1 @ 0.50 (books cash -0.50), then the book goes one-sided while we still hold it
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, {"A": (0.50, 0.50)}, 0.0)
    cash2, inv2, pnl2, _, fills2 = _mark_step(T, base, last, {"A": 1.0}, {"A": (None, None)}, cash)
    assert fills2 == [] and round(cash2, 4) == -0.50
    assert round(pnl2, 4) == -0.50, "a long we cannot mark is worth 0 at worst — a full loss of cost"


def test_an_unmarkable_SHORT_does_NOT_read_as_a_win():
    """THE KILL-SWITCH DEFECT. Skipping the mark left the short's +0.50 proceeds in cash with
    nothing against them, so P&L read +0.50 — a WIN — and `pnl < -loss_cap` could never fire on a
    position that had become impossible to exit."""
    base = {"A": 0.0}
    # ⚠️ A REAL SPREAD, not (0.50, 0.50). A zero-spread book makes bid ≡ ask, so it cannot tell a
    # side-selection error from correct behaviour — this test would pass with `bid`/`ask` swapped.
    # The maker SELLS at its ask (0.51), so cash is +0.51 and the worst case is -0.49.
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": -1.0}, {"A": (0.49, 0.51)}, 0.0)
    assert round(cash, 4) == 0.51, "a maker SELL books at its own ask, not the bid or the mid"
    cash2, inv2, pnl2, _, _ = _mark_step(T, base, last, {"A": -1.0}, {"A": (None, None)}, cash)
    assert round(pnl2, 4) == -0.49, (
        "an unmarkable SHORT must be valued at 1.00 against us (worst case), i.e. -0.49 P&L — "
        "skipping it reports +0.51 and disarms the loss cap")
    assert pnl2 < 0, "an unmarkable short must never report a profit"


# ── M13: a SETTLED ticker VANISHES from /portfolio/positions — that is NOT a flatten ─────────────
# The venue returns a flat-but-LIVE market at position_fp 0.00, but DROPS a settled one entirely.
# `cur.get(t, 0.0)` conflated them, so a vanished held position booked d = 0 − prev — a phantom SELL
# of the whole position at the ask, inventing cash the account never received and reporting a
# completed profitable round trip (the direction that disarms the loss cap). A disappearance is never
# a trade; the vanished position is valued WORST-CASE, exactly like a one-sided book.

def test_a_VANISHED_held_long_is_worst_case_marked_not_booked_as_a_flatten():
    base = {"A": 0.0}
    # buy 1 @ 0.50 (cash -0.50, held long 1), then the ticker SETTLES and vanishes from the read.
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, {"A": (0.50, 0.50)}, 0.0)
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {}, {"A": (0.49, 0.51)}, cash)
    assert fills2 == [], "a disappearance must NOT book a phantom sell"
    assert round(cash2, 4) == -0.50, "no invented proceeds — cash unchanged by the disappearance"
    assert round(pnl2, 4) == -0.50, "a vanished long is worst-case 0 → a full loss of cost, not a win"
    assert inv2["A"] == 1.0, "still shown held, not reported flat"
    assert last2["A"] == 1.0, "last carried (not advanced away) so a later read can still resolve it"


def test_a_VANISHED_held_short_does_NOT_read_as_a_win():
    """The dangerous direction: a vanished SHORT booked a phantom BUY that closed it at a profit,
    reporting flat + gain and disarming the cap on a position that had actually settled."""
    base = {"A": 0.0}
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": -1.0}, {"A": (0.49, 0.51)}, 0.0)
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {}, {"A": (0.49, 0.51)}, cash)
    assert fills2 == [], "no phantom buy-to-close from a disappearance"
    assert round(pnl2, 4) == -0.49, "worst-case 1.00 against the short → -0.49, never the +0.51 win"
    assert inv2["A"] == -1.0, "still shown held short"


def test_a_flat_but_LIVE_ticker_at_zero_is_NOT_treated_as_vanished():
    """The distinction that makes the fix safe: a REAL flatten (we sold, venue reports 0.00) must
    still book normally — only an ABSENT ticker is the ambiguous disappearance."""
    base = {"A": 0.0}
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, {"A": (0.50, 0.50)}, 0.0)
    # we SELL it back (venue now reports A at 0.0, present in the dict) → a real flatten, booked
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {"A": 0.0}, {"A": (0.50, 0.50)}, cash)
    assert fills2 == [("A", -1.0, 0.50)], "a present-at-zero ticker is a real flatten, still booked"
    assert inv2["A"] == 0.0 and round(cash2, 4) == 0.0


def test_a_BOOKED_short_is_still_worst_case_marked_when_a_NEW_fill_cannot_be_valued():
    """THE CASE THE FIRST ATTEMPT MISSED, and the one that actually produces a disarmed cap.

    The skip is keyed on `d != 0`, which is a property of the TICKER, not of the new contract — so
    an unbooked fill used to suppress the worst-case mark for every already-booked contract on that
    ticker too. And because `last` is deliberately never advanced, `d` stays non-zero every
    subsequent cycle, so the disarm lasted the whole one-sided window rather than one cycle.

    The correlation is what makes this the realistic case rather than a corner: a taker sweeping one
    side of our quote is precisely what leaves the book one-sided, so the fill and the `None` mid
    arrive together."""
    base = {"A": 0.0}
    # Real spread (a zero-spread book cannot distinguish a side-selection error): the sell books at
    # the ask 0.51, so cash is +0.51 and the booked short's worst case is -0.49.
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": -1.0}, {"A": (0.49, 0.51)}, 0.0)
    assert round(cash, 4) == 0.51 and last["A"] == -1.0
    # a second sell fills while the book is one-sided
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {"A": -2.0}, {"A": (None, None)}, cash)
    assert fills2 == [] and round(cash2, 4) == 0.51, "the unvaluable fill must not book cash"
    assert last2.get("A", 0.0) == -1.0, "last must NOT advance — the new fill re-books later"
    assert round(pnl2, 4) == -0.49, (
        "the ALREADY-BOOKED short must still be marked at 1.00 against us; suppressing it because "
        "a different contract could not be valued reports +0.51 and disarms the cap")
    assert pnl2 < 0, "must never report a profit while holding unmarkable shorts"


# ── EXIT-SIDE MARKING: price each part on the side that actually transacts it ────────────────────
# `quotes[t]` is `(bid, ask)`. A maker buys at its bid and sells at its ask, and a position
# liquidates the same way round, so one rule prices both the fill and the inventory it created.

def test_a_MAKER_ROUND_TRIP_captures_the_spread_instead_of_reporting_zero():
    """⚠️ THE REASON EXIT-SIDE MARKING MATTERS — mid-marking is structurally BLIND to spread
    capture, which is the entire thing a market maker earns.

    Buy 1 at the bid (0.40), sell it back at the ask (0.45): a textbook maker round trip that
    captured a full <n> spread. Booking BOTH legs at the mid (0.425) nets exactly zero — the tool
    would report $0.00 on a trade that made a nickel. Pricing each leg on the side that actually
    filled reports the <n>.

    ⚠️ DO NOT cite the clean-lane run as evidence for this. It is the tempting example and it is
    the wrong one: the venue's own `realized_pnl_dollars` was ALSO 0.000000 there, because exactly
    one contract round-tripped (0.40→0.40) and the rest were left open. The clean lane really did
    capture ~nothing. The runs where this blindness actually bites are the ones that COMPLETED round
    trips — PERLIGA (venue realized 0.080000) and MLBKS (0.063400) — where the tool's mid-booked
    number understates the venue's. Two separate facts; an earlier version of this docstring merged
    them and stated the venue showed "a positive figure" for the clean lane, which is false."""
    base = {"A": 0.0}
    q = {"A": (0.40, 0.45)}
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, q, 0.0)
    assert fills == [("A", 1.0, 0.40)], "a maker BUY fills at its own bid, not at the mid"
    assert round(cash, 4) == -0.40 and round(pnl, 4) == 0.0, "P&L is 0 at the fill, not half a spread"
    cash2, inv2, pnl2, _, fills2 = _mark_step(T, base, last, {"A": 0.0}, q, cash)
    assert fills2 == [("A", -1.0, 0.45)], "a maker SELL fills at its own ask"
    assert round(pnl2, 4) == 0.05, "the round trip captured the spread; mid-marking reports 0.00"


def test_a_LONG_marks_at_the_BID_because_that_is_where_it_would_exit():
    """Mid-marking overstates a long by half a spread — value you could not realise by selling."""
    base = {"A": 0.0}
    q = {"A": (0.40, 0.45)}
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": 2.0}, q, 0.0)
    assert round(cash, 4) == -0.80                      # 2 bought at the bid
    assert round(pnl, 4) == 0.0                         # marked at the same bid → flat, honestly
    # the bid improves by a cent with the ask unchanged → a real, realisable gain of <n>
    _c, _i, pnl2, _l, _f = _mark_step(T, base, last, {"A": 2.0}, {"A": (0.41, 0.45)}, cash)
    assert round(pnl2, 4) == 0.02


def test_a_ONE_SIDED_book_still_marks_the_side_it_can_price():
    """The old `mid`-based rule condemned the WHOLE ticker whenever either ladder was empty. An
    empty NO ladder leaves a perfectly good bid, so a LONG is still markable — and the feed's
    fabricated `yes_ask = 1.0` makes a SHORT fall back to its worst case with no special branch."""
    base = {"A": 0.0}
    one_sided = {"A": (0.40, 1.0)}                      # NO ladder empty → ask fabricated to 1.0
    _c, _i, pnl_long, _l, _f = _mark_step(T, base, {"A": 1.0}, {"A": 1.0}, one_sided, -0.40)
    assert round(pnl_long, 4) == 0.0, "a long marks at the real bid — not 'unmarkable'"
    _c2, _i2, pnl_short, _l2, _f2 = _mark_step(T, base, {"A": -1.0}, {"A": -1.0}, one_sided, 0.50)
    assert round(pnl_short, 4) == -0.50, "a short buys back at the fabricated 1.0 = its worst case"


def test_a_FILL_is_NEVER_booked_at_the_fabricated_placeholder_price():
    """⚠️ THE ASYMMETRY THAT ALMOST SHIPPED: the fabricated bound is a valid MARK and an invalid
    FILL PRICE, and one rule cannot serve both.

    `feed._derive` writes `yes_bid = 0.0` for an empty YES ladder and `yes_ask = 1.0` for an empty
    NO ladder. Marking held inventory there is the settlement bound — conservative, and exactly what
    M1 wants. Booking a FILL there is the most optimistic price that exists: a buy recorded as FREE,
    a sell recorded at <n>. And because booking advances `last`, it is permanent — unlike the old
    code, which refused the placeholder and re-booked when the ladder returned.

    Measured before the guard: a short sold into an empty NO ladder booked +<n> and printed
    +<n> of phantom profit once the book recovered; a long booked $0.00 and printed +<n>. Both
    in the direction that disarms the loss cap."""
    base = {"A": 0.0}
    empty_no = {"A": (0.44, 1.0)}                 # NO ladder empty → ask fabricated
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 0.0}, {"A": -1.0}, empty_no, 0.0)
    assert fills == [], "a sell must NOT be booked at the fabricated 1.00"
    assert round(cash, 4) == 0.0, "no cash may be booked at a fictional price"
    assert last.get("A", 0.0) == 0.0, "last must NOT advance — the fill re-books when the book returns"
    # the book recovers: the fill books now, at a real price
    cash2, _i2, pnl2, last2, fills2 = _mark_step(T, base, last, {"A": -1.0}, {"A": (0.44, 0.46)}, cash)
    assert fills2 == [("A", -1.0, 0.46)] and round(cash2, 4) == 0.46
    assert round(pnl2, 4) == 0.0, "booked and marked at the same ask → flat, not +0.54 of phantom gain"

    empty_yes = {"A": (0.0, 0.45)}                # YES ladder empty → bid fabricated
    c3, _i3, _p3, last3, fills3 = _mark_step(T, base, {"A": 0.0}, {"A": 1.0}, empty_yes, 0.0)
    assert fills3 == [] and round(c3, 4) == 0.0 and last3.get("A", 0.0) == 0.0, (
        "a buy must NOT be booked as free against an empty YES ladder")


def test_HELD_inventory_still_marks_at_the_fabricated_bound():
    """The other half of the asymmetry: refusing the placeholder as a FILL price must not stop it
    being used as a MARK, or the M1 worst-case bound regresses.

    ⚠️ THE SHAPE MATTERS. With no fill this cycle (`d == 0`) the worst-case fallback returns the
    identical number, so the assertion cannot tell the two apart and the mutant it names survives.
    The discriminating shape is a BOOKABLE fill coexisting with placeholder-marked inventory: here a
    buy closes 1 of a short 2 at the real bid 0.44 while the remaining short marks at the fabricated
    1.00. Marking with `as_fill=True` would refuse that mark and drop the whole ticker to the
    fallback, booking nothing — a visibly different number."""
    base = {"A": 0.0}
    # held short 2 (booked at 0.50 each = +1.00 cash); NO ladder empty; a buy closes 1 at bid 0.44
    cash, inv, pnl, last, fills = _mark_step(
        T, base, {"A": -2.0}, {"A": -1.0}, {"A": (0.44, 1.0)}, 1.00)
    assert fills == [("A", 1.0, 0.44)], "the closing buy IS bookable at the real bid"
    assert round(pnl, 4) == -0.44, (
        "the remaining short must still mark at the fabricated 1.00; refusing it as a mark drops "
        "the ticker to the worst-case fallback and books no fill at all (-1.00)")


def test_a_CROSSED_book_is_not_a_price():
    """`_mid_from` refuses `bid > ask`; this path had no such check, so a long marked off the
    inflated bid of the real captured crossed shape (0.65/0.61) for a phantom +<n> — the direction
    that disarms the cap. A zero-spread book (`bid == ask`) is legitimate and must still price."""
    base = {"A": 0.0}
    _c, _i, pnl, _l, _f = _mark_step(T, base, {"A": 3.0}, {"A": 3.0}, {"A": (0.65, 0.61)}, -1.50)
    assert round(pnl, 4) == -1.50, "a crossed book must fall back to the worst case, not mark +0.45"
    _c2, _i2, pnl2, _l2, _f2 = _mark_step(T, base, {"A": 1.0}, {"A": 1.0}, {"A": (0.50, 0.50)}, -0.50)
    assert round(pnl2, 4) == 0.0, "a zero-spread book is legitimate and must still price"


def test_a_ROUND_TRIP_closed_on_an_unpriceable_book_does_not_print_a_profit():
    """The counterexample that broke the previous attempt's END-OF-RUN number.

    Sell 1 @ 0.90 (cash +0.90). The NO ladder then empties — `_mid_from` returns None — and our
    resting BUY fills, taking us back to FLAT. Because the ticker is now flat it belonged to
    NEITHER the `marked` nor the `unmarkable` list, so the old inline teardown formula returned
    `cash` untouched: **+<n> on a round trip that made about a cent**, with no warning printed.

    `_mark_step` gets it right because it keys off BOOKED inventory (`prev − base`), not off current
    inventory — the short is still booked even though we are flat. The teardown now calls this
    function instead of re-deriving, so the two numbers cannot diverge again."""
    base = {"A": 0.0}
    cash, inv, pnl, last, _ = _mark_step(T, base, {"A": 0.0}, {"A": -1.0}, {"A": (0.90, 0.90)}, 0.0)
    assert round(cash, 4) == 0.90 and last["A"] == -1.0
    cash2, inv2, pnl2, last2, fills2 = _mark_step(T, base, last, {"A": 0.0}, {"A": (None, None)}, cash)
    assert inv2["A"] == 0.0, "we are flat"
    assert round(pnl2, 4) == -0.10, (
        "the BOOKED short must still be marked even though current inventory is flat; keying off "
        "`inv` instead of `prev - base` returns cash untouched and prints +0.90")
    assert pnl2 < 0.90, "must never report the unopposed sale proceeds as profit"


def test_worst_case_marking_only_applies_to_HELD_inventory():
    """Flat inventory with no mid must contribute nothing — the worst case of zero contracts is zero.

    ⚠️ It does NOT pin the `if booked:` guard, despite reading as if it does: deleting that guard
    leaves this green, because `0.0 × 1.0 == 0.0`. What it kills is a CONSTANT worst-case term (one
    not scaled by the position), which would fire the cap on an empty book."""
    base = {"A": 0.0}
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": 0.0}, {"A": 0.0}, {"A": (None, None)}, 0.0)
    assert inv["A"] == 0.0 and fills == [] and round(pnl, 4) == 0.0


# ── the loss kill-switch must not be silently disabled by a garbage venue value ──────────
# A NaN position parses fine (float("NaN") succeeds), poisons cash -> pnl, and `nan < -cap` is
# False — so the <n> kill-switch on a REAL-MONEY run would look armed while protecting nothing.
# This is the same defect fixed in bot/core/safety.py; it was still open here.
import math

import pytest


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity"])
@pytest.mark.asyncio
async def test_non_finite_position_reads_as_CANNOT_VERIFY(bad):
    """A successfully-read GARBAGE value is as unverifiable as a failed read, so it must take the
    same fail-closed path (None -> the caller halts + cancels all)."""
    from bot.kalshi import maker as mm

    class _C:
        async def get_positions(self):
            return [{"ticker": "A", "position_fp": bad}]

    assert await mm._positions(_C()) is None, bad


@pytest.mark.asyncio
async def test_a_normal_position_still_reads_fine():
    """Control — the guard must not reject real holdings, including the fractional values the
    2026-07-19 live run actually returned."""
    from bot.kalshi import maker as mm

    class _C:
        async def get_positions(self):
            return [{"ticker": "A", "position_fp": "-4.65"}, {"ticker": "B", "position_fp": "3.00"}]

    from decimal import Decimal as _D
    assert await mm._positions(_C()) == {"A": _D("-4.65"), "B": _D("3.00")}


# ── Phase D step 2: the position/P&L path is EXACT Decimal ───────────────────────────────────────

@pytest.mark.asyncio
async def test_positions_are_parsed_from_the_wire_STRING_into_exact_decimals():
    """`position_fp` arrives as a decimal STRING and is the quantity every money number downstream
    is built from — cash, the mark, the loss cap, the inventory cap. `float("-4.65")` throws the
    exactness away before we get a chance to keep it (CLAUDE.md § Code style), and the venue really
    does partial-fill below one contract, so these are not always round numbers.

    Scale is the discriminator, not value: `float(-4.65) == Decimal("-4.65")` is True, so a
    value-only assertion is satisfied by the float round-trip this removes."""
    from decimal import Decimal

    from bot.kalshi import maker as mm

    class _C:
        async def get_positions(self):
            return [{"ticker": "A", "position_fp": "-4.65"}, {"ticker": "B", "position_fp": "3.00"}]

    got = await mm._positions(_C())
    assert all(isinstance(v, Decimal) for v in got.values())
    assert str(got["A"]) == "-4.65" and str(got["B"]) == "3.00"


@pytest.mark.asyncio
async def test_an_absurd_position_magnitude_is_CANNOT_VERIFY_not_an_accepted_position():
    """The guard the exactness migration would otherwise have QUIETLY REMOVED. The float path read
    `float("1E+400")` as `inf`, which `math.isfinite` caught and routed to the fail-closed halt;
    `Decimal("1E+400")` is a perfectly FINITE Decimal, so the `is_finite()` check that replaced it
    waves the same wire value straight through into cash → pnl, where it pins (or, sign-flipped,
    disarms) the loss cap on the first cycle.

    Asserted as None — the CANNOT-VERIFY halt path — and NOT merely as absent from the dict: a
    non-target skip would also leave it absent, and that is the wrong direction for our own market."""
    from bot.kalshi import maker as mm

    class _C:
        async def get_positions(self):
            return [{"ticker": "A", "position_fp": "1E+400"}]

    assert await mm._positions(_C(), targets={"A"}) is None

    # The control: an ordinary large-but-real position is still a position, so the bound cannot be
    # satisfied by a guard mutated to refuse everything.
    class _Sane:
        async def get_positions(self):
            return [{"ticker": "A", "position_fp": "1000000"}]

    assert await mm._positions(_Sane(), targets={"A"}) == {"A": D("1000000")}


def test_mark_step_is_exact_when_fed_exact_positions_and_quotes():
    """The arithmetic the loss cap is evaluated on. `3 × 0.47` is 1.4100000000000001 in binary
    float, so `cash` drifts from the first fill onward and every later comparison inherits it —
    including `pnl < -loss_cap`, which is a compare across a money threshold.

    ⚠️ Asserted on `str()`, not on a rounded value: `round(cash, 4) == -1.41` passes for the float
    too. Only the exact representation distinguishes them."""
    from decimal import Decimal as D

    base = {"A": D(0)}
    q = {"A": (D("0.47"), D("0.49"))}
    cash, inv, pnl, last, fills = _mark_step(T, base, {"A": D(0)}, {"A": D("3.00")}, q, D(0))
    assert isinstance(cash, D) and isinstance(pnl, D) and isinstance(inv["A"], D)
    assert str(cash) == "-1.4100", "3 × 0.47 booked exactly, not 1.4100000000000001"
    assert pnl == 0, "P&L is exactly zero at the fill — exit-side booking and marking cancel"
    assert fills == [("A", D("3.00"), D("0.47"))]


def test_an_unmarkable_position_is_worst_cased_exactly_in_decimal():
    """The settlement-bound branch (`long → 0`, `short → 1`) is the one place `_mark_step` writes a
    literal into the money total. Float literals there would re-infect an exact `cash`."""
    from decimal import Decimal as D

    base = {"A": D(0)}
    cash, _i, _p, last, _f = _mark_step(T, base, {"A": D(0)}, {"A": D("-1.00")},
                                        {"A": (D("0.49"), D("0.51"))}, D(0))
    assert str(cash) == "0.5100", "a maker SELL books at its own ask, exactly"
    cash2, _i2, pnl2, _l2, _f2 = _mark_step(T, base, last, {"A": D("-1.00")},
                                            {"A": (None, None)}, cash)
    assert isinstance(pnl2, D) and str(pnl2) == "-0.4900", (
        "an unmarkable short is worth 1.00 against us; the bound must arrive as an exact Decimal")



def _fill(side, action, yes_px, no_px):
    return {"fill_id": f"{side}{action}{yes_px}", "ticker": "T", "side": side, "action": action,
            "yes_price_dollars": yes_px, "no_price_dollars": no_px,
            "count_fp": "1.00", "fee_cost": "0.000000", "is_taker": False, "ts": 1}



