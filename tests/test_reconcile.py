"""Venue-vs-tracker position reconciliation — the "am I stranded without knowing?" guard.

The tracker is a LOCAL BELIEF built from order responses; nothing used to ask either venue what we
ACTUALLY hold, so a lost order response (30s timeout / 502 / reset AFTER the engine filled) or a
crash between fill and add_position left a real position invisible to us — no alert, no pause, no
cap. These pin the decision rules: what counts as unknown exposure, what the whitelist may and may
not silence, and that a read failure is never mistaken for "flat".
"""
import json

import pytest

from bot.core.reconcile import (
    Divergence, VenuePosition, _first_qty, find_divergences, known_quantities,
    load_whitelist, poly_slug, recon_token, save_whitelist, tracker_keys,
)


class _Pos:
    """Stand-in for OpenPosition (only token_id/shares matter here)."""
    def __init__(self, token_id, shares):
        self.token_id, self.shares = token_id, shares


# ── the core check: venue > known ─────────────────────────────────────────────────────────────
def test_venue_position_we_know_nothing_about_is_a_divergence():
    """THE case this exists for: a lost order response left a real position we never recorded."""
    known = known_quantities([], {})
    divs = find_divergences([VenuePosition("kalshi", "KX-AB-A", 3.0)], known)
    assert len(divs) == 1
    assert divs[0].unknown == 3.0


def test_venue_matching_the_tracker_is_not_a_divergence():
    known = known_quantities([_Pos("KX-AB-A", 10.0)], {})
    assert find_divergences([VenuePosition("kalshi", "KX-AB-A", 10.0)], known) == []


def test_venue_holding_MORE_than_the_tracker_diverges_by_the_excess():
    """A partial record: we know about 10, the venue has 13 → 3 are unknown."""
    known = known_quantities([_Pos("KX-AB-A", 10.0)], {})
    divs = find_divergences([VenuePosition("kalshi", "KX-AB-A", 13.0)], known)
    assert divs[0].unknown == 3.0


def test_venue_holding_LESS_is_NOT_flagged():
    """v1 checks ONE direction. venue < known is dominated by ordinary settlement (the tracker
    keeps rows for settled games), so flagging it would be a false-positive storm that pauses the
    bot constantly. It IS a real signal and is v2 — see the module docstring."""
    known = known_quantities([_Pos("KX-AB-A", 10.0)], {})
    assert find_divergences([VenuePosition("kalshi", "KX-AB-A", 4.0)], known) == []


def test_poly_short_and_long_rows_aggregate_against_the_one_venue_row():
    """Tracker holds `slug` and `slug::short`; the venue reports ONE row per slug (short is a sign
    on netPosition). Both must credit the same key or a legitimate hedge looks unknown."""
    known = known_quantities([_Pos("aec-mlb-tor-bos", 6.0), _Pos("aec-mlb-tor-bos::short", 4.0)], {})
    assert find_divergences([VenuePosition("poly", "aec-mlb-tor-bos", 10.0)], known) == []
    assert len(find_divergences([VenuePosition("poly", "aec-mlb-tor-bos", 11.0)], known)) == 1


# ── the whitelist: acknowledge, never blanket-ignore ──────────────────────────────────────────
def test_whitelist_acknowledges_a_known_manual_position():
    known = known_quantities([], {("poly", "tec-mls-x"): 255.0})
    assert find_divergences([VenuePosition("poly", "tec-mls-x", 255.0)], known) == []


def test_whitelist_does_NOT_silence_a_CHANGE_to_an_acknowledged_position():
    """THE reason this is an acknowledge-list, not an ignore-list. You signed off on 255. If it
    becomes 300, the extra 45 is NOT what you acknowledged — a blanket 'ignore this market' would
    hide it, and would equally hide a genuine future strand on that market."""
    known = known_quantities([], {("poly", "tec-mls-x"): 255.0})
    divs = find_divergences([VenuePosition("poly", "tec-mls-x", 300.0)], known)
    assert len(divs) == 1
    assert divs[0].unknown == 45.0


def test_whitelist_is_scoped_to_its_venue():
    """Acknowledging poly/X must not silence kalshi/X."""
    known = known_quantities([], {("poly", "X"): 5.0})
    assert len(find_divergences([VenuePosition("kalshi", "X", 5.0)], known)) == 1


def test_whitelist_roundtrips(tmp_path):
    p = str(tmp_path / "wl.json")
    save_whitelist(p, {("poly", "a"): 3.0, ("kalshi", "B"): 7.0}, note="test")
    assert load_whitelist(p) == {("poly", "a"): 3.0, ("kalshi", "B"): 7.0}


def test_missing_whitelist_acknowledges_nothing(tmp_path):
    """Absent file → {} → everything still alerts. The SAFE default."""
    assert load_whitelist(str(tmp_path / "nope.json")) == {}


def test_corrupt_whitelist_acknowledges_nothing_rather_than_everything(tmp_path):
    """A corrupt file must NOT fail open (silently acknowledging real positions). Empty = loud."""
    p = tmp_path / "wl.json"
    p.write_text("{ this is not json")
    assert load_whitelist(str(p)) == {}


def test_malformed_entry_is_skipped_not_fatal(tmp_path):
    p = tmp_path / "wl.json"
    p.write_text(json.dumps({"entries": [
        {"venue": "poly", "market": "ok", "qty": 2},
        {"venue": "poly", "no_qty_field": True},          # malformed → skipped
    ]}))
    assert load_whitelist(str(p)) == {("poly", "ok"): 2.0}


# ── fail direction: unparseable ≠ flat ────────────────────────────────────────────────────────
def test_first_qty_prefers_position_fp_and_returns_None_when_unreadable():
    """Kalshi's field is `position_fp` (fixed-point STRING); `position` is the older int. Reading
    the wrong one reports "flat" on a real position — this module's whole failure mode, and a trap
    this project has already hit once. Unparseable → None (CANNOT VERIFY), never 0.0."""
    assert _first_qty({"position_fp": "-3.00", "position": 0}, "position_fp", "position") == 3.0
    assert _first_qty({"position": 5}, "position_fp", "position") == 5.0
    assert _first_qty({"position_fp": ""}, "position_fp", "position") is None
    assert _first_qty({"unexpected_rename": "3"}, "position_fp", "position") is None
    assert _first_qty({}, "position_fp", "position") is None


def test_first_qty_is_absolute_so_a_short_still_counts_as_held():
    """A NO position is a NEGATIVE YES position (position_fp=-1.00 == long 1 NO). Sign is
    irrelevant to "do we hold something we don't know about" — magnitude is what matters."""
    assert _first_qty({"position_fp": "-255.00"}, "position_fp") == 255.0


# ── synthetic strand rows must not clobber real ones ──────────────────────────────────────────
def test_recon_token_cannot_collide_with_a_real_market_id():
    """add_stranded does `self._positions[token_id] = ...` — a plain dict OVERWRITE. A synthetic
    row keyed by the real market id would DESTROY a live hedged position row (losing its cost
    basis, corrupting the exposure caps)."""
    tok = recon_token("kalshi", "KX-AB-A")
    assert tok != "KX-AB-A"
    assert tok.startswith("reconcile::")


def test_a_recorded_divergence_stops_re_escalating():
    """Once recorded as a strand, the position is KNOWN — it must stop counting as unknown every
    poll (its strand keeps the pause). Otherwise it re-alerts forever."""
    recorded = _Pos(recon_token("kalshi", "KX-AB-A"), 3.0)
    known = known_quantities([recorded], {})
    assert find_divergences([VenuePosition("kalshi", "KX-AB-A", 3.0)], known) == []


def test_recon_row_credits_only_its_own_venue():
    known = known_quantities([_Pos(recon_token("kalshi", "X"), 3.0)], {})
    assert len(find_divergences([VenuePosition("poly", "X", 3.0)], known)) == 1


def test_tracker_keys_credits_both_venues_for_a_real_row():
    """We cannot tell a Poly token from a Kalshi ticker by shape alone, so a real row credits both.
    Over-crediting only ever suppresses a false alarm; it can't hide a venue position exceeding
    everything we know about."""
    assert set(tracker_keys("KX-AB-A")) == {("kalshi", "KX-AB-A"), ("poly", "KX-AB-A")}
    assert set(tracker_keys("slug::short")) == {("kalshi", "slug::short"), ("poly", "slug")}


def test_poly_slug_strips_the_short_suffix():
    assert poly_slug("aec-mlb-tor-bos::short") == "aec-mlb-tor-bos"
    assert poly_slug("aec-mlb-tor-bos") == "aec-mlb-tor-bos"


# ── the fetchers: CANNOT VERIFY is never "flat" ───────────────────────────────────────────────
import types
from unittest.mock import AsyncMock, MagicMock

from bot.core.reconcile import ReconcileMixin


def _mixin(*, kalshi=None, poly_pages=None, kalshi_raises=False, poly_raises=False):
    r = ReconcileMixin()
    r._poly_us = True
    r.kalshi_client = MagicMock()
    if kalshi_raises:
        r.kalshi_client.get_positions = AsyncMock(side_effect=RuntimeError("502"))
    else:
        r.kalshi_client.get_positions = AsyncMock(return_value=kalshi if kalshi is not None else [])
    pages = list(poly_pages or [{"positions": {}, "eof": True, "nextCursor": ""}])
    async def positions(params=None):
        if poly_raises:
            raise RuntimeError("timeout")
        return pages.pop(0) if pages else {"positions": {}, "eof": True, "nextCursor": ""}
    r.client = MagicMock()
    r.client._sdk = types.SimpleNamespace(portfolio=types.SimpleNamespace(positions=positions))
    return r


@pytest.mark.asyncio
async def test_kalshi_read_failure_is_cannot_verify_not_empty(monkeypatch):
    """A read error must NOT read as "confirmed flat" — that is exactly the blindness we're
    fixing. None = cannot verify."""
    from bot.core import config
    monkeypatch.setattr(config, "KALSHI_API_KEY", "k")
    r = _mixin(kalshi_raises=True)
    assert await r._fetch_kalshi_positions() is None


@pytest.mark.asyncio
async def test_kalshi_unparseable_position_is_cannot_verify(monkeypatch):
    """A field RENAME (position_fp → something else) must not silently report flat."""
    from bot.core import config
    monkeypatch.setattr(config, "KALSHI_API_KEY", "k")
    r = _mixin(kalshi=[{"ticker": "KX-A", "renamed_qty_field": "3"}])
    assert await r._fetch_kalshi_positions() is None


@pytest.mark.asyncio
async def test_kalshi_parses_position_fp(monkeypatch):
    from bot.core import config
    monkeypatch.setattr(config, "KALSHI_API_KEY", "k")
    r = _mixin(kalshi=[{"ticker": "KX-A", "position_fp": "-3.00"},
                       {"ticker": "KX-B", "position_fp": "0.00"}])   # 0 → not held
    out = await r._fetch_kalshi_positions()
    assert [(p.venue, p.market, p.qty) for p in out] == [("kalshi", "KX-A", 3.0)]


@pytest.mark.asyncio
async def test_poly_read_failure_is_cannot_verify():
    r = _mixin(poly_raises=True)
    assert await r._fetch_poly_positions() is None


@pytest.mark.asyncio
async def test_poly_paginates_to_eof():
    """Stopping at page 1 would MISS positions — reproducing the very bug this guards against."""
    r = _mixin(poly_pages=[
        {"positions": {"slug-a": {"netPosition": "5"}}, "eof": False, "nextCursor": "c1"},
        {"positions": {"slug-b": {"netPosition": "7"}}, "eof": True, "nextCursor": ""},
    ])
    out = await r._fetch_poly_positions()
    assert sorted((p.market, p.qty) for p in out) == [("slug-a", 5.0), ("slug-b", 7.0)]


@pytest.mark.asyncio
async def test_poly_unparseable_position_is_cannot_verify():
    r = _mixin(poly_pages=[{"positions": {"slug-a": {"renamed": "5"}}, "eof": True}])
    assert await r._fetch_poly_positions() is None


@pytest.mark.asyncio
async def test_reconcile_once_reports_unverified_so_empty_is_not_all_clear(monkeypatch):
    """If a venue can't be read, an EMPTY divergence list must not be mistaken for all-clear."""
    from bot.core import config
    monkeypatch.setattr(config, "KALSHI_API_KEY", "k")
    monkeypatch.setattr(config, "RECONCILE_WHITELIST_FILE", "/nonexistent/wl.json")
    r = _mixin(kalshi_raises=True)
    r.tracker = MagicMock()
    r.tracker.list_positions.return_value = []
    divs, verified = await r._reconcile_once()
    assert divs == [] and verified is False


def test_first_qty_prefers_the_exact_decimal_field_and_falls_back():
    """[box probe 2026-08-06 / joint review N6] netPositionDecimal is the exact holding;
    the rounded netPosition stays as fallback HERE only (arb-side reader, prepend-only —
    see the call-site comment). Divergent pin so the prepend is not a no-op to the suite."""
    from bot.core.reconcile import _first_qty
    assert _first_qty({"netPosition": "11", "netPositionDecimal": "10.6000"},
                      "netPositionDecimal", "netPosition", "qtyAvailable") == 10.6
    assert _first_qty({"netPosition": "11"},
                      "netPositionDecimal", "netPosition", "qtyAvailable") == 11.0
