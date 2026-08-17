"""The recovery decision — what a supervisor or the next start does with the durable state.

This is the SEPARATE path the SIGKILL case requires. In-process cleanup is unavailable by
definition, so recovery has to be something a *different* process runs: read the durable record,
ask the venue what is actually there, and decide.

THE ONE RULE, INHERITED FROM AN EARLIER POSITION RECONCILER: **cannot-verify is not flat.** A
venue read that fails must never resolve to "nothing there, safe to start". That reconciler's
Kalshi half returned `[]` on an unparsed response for a MONTH and reported "confirmed flat" every
poll while we held positions; the whole failure was one line upstream of every guard that
depended on it.

THE SECOND RULE, which is this module's own: **cancelling is safe, flattening is not.** A cancel
only ever removes exposure, so an unattributable resting order can be cancelled automatically. A
flatten MOVES MONEY against a basis we may not own — so a position we have no record of is an
operator decision, exactly as the reconciler treats unknown exposure.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from bot.core import maker_state
from bot.core.maker_state import MakerState, assess_recovery


def _state(**kw) -> MakerState:
    base = dict(run_id="r1", pid=123, mode="real", started_ts=1.0, updated_ts=2.0,
                clean_exit=True, tickers=("KXA",), orders={}, inventory={},
                realized_pnl=Decimal("0"), loss_cap=Decimal("5"), exit_status="clean")
    base.update(kw)
    return MakerState(**base)


# ── cannot-verify dominates everything ───────────────────────────────────────────────────────

def test_unreadable_venue_POSITIONS_refuses_to_start(tmp_path):
    plan = assess_recovery(_state(), venue_orders=[], venue_positions=None)
    assert plan.action == "refuse"
    assert plan.reason == "cannot_verify"


def test_unreadable_venue_ORDERS_refuses_to_start():
    plan = assess_recovery(_state(), venue_orders=None, venue_positions={})
    assert plan.action == "refuse"
    assert plan.reason == "cannot_verify"


def test_cannot_verify_beats_an_otherwise_perfectly_clean_prior_run():
    """The clean flag describes the LAST process. It says nothing about what is on the venue now."""
    plan = assess_recovery(_state(clean_exit=True), venue_orders=None, venue_positions=None)
    assert plan.action == "refuse"


def test_an_EMPTY_venue_read_is_NOT_the_same_as_an_unreadable_one():
    """`[]` means confirmed flat and must be allowed to start — otherwise the guard is unusable
    and gets disabled. The distinction is the entire point."""
    assert assess_recovery(_state(), venue_orders=[], venue_positions={}).action == "start"


# ── the SIGKILL case ─────────────────────────────────────────────────────────────────────────

def test_an_unclean_exit_with_orders_still_resting_demands_recovery():
    plan = assess_recovery(
        _state(clean_exit=False, exit_status=None,
               orders={"i1": {"order_id": "oid-9", "ticker": "KXA", "side": "yes",
                              "price": "0.40", "count": 1}}),
        venue_orders=[{"order_id": "oid-9", "ticker": "KXA"}],
        venue_positions={},
    )
    assert plan.action == "recover"
    assert plan.cancel_order_ids == ("oid-9",)


def test_an_unclean_exit_with_a_RECORDED_position_flattens_it():
    plan = assess_recovery(
        _state(clean_exit=False, exit_status=None, inventory={"KXA": Decimal("3")}),
        venue_orders=[],
        venue_positions={"KXA": Decimal("3")},
    )
    assert plan.action == "recover"
    assert plan.flatten == (("KXA", Decimal("3")),)


def test_a_CLEAN_exit_that_nevertheless_left_a_resting_order_still_demands_recovery():
    """The flag is evidence, not truth. The venue is truth. A clean-looking exit that left an
    order resting is exactly the recorded 2xx-with-no-order_id case — the order
    was never in our list to cancel."""
    plan = assess_recovery(
        _state(clean_exit=True),
        venue_orders=[{"order_id": "ghost-1", "ticker": "KXA"}],
        venue_positions={},
    )
    assert plan.action == "recover"
    assert plan.cancel_order_ids == ("ghost-1",)


def test_an_unclean_exit_with_a_confirmed_flat_venue_may_start():
    """Refusing here would wedge the maker permanently after any crash, and a guard that always
    refuses gets removed. Confirmed-flat is confirmed."""
    plan = assess_recovery(_state(clean_exit=False, exit_status=None),
                           venue_orders=[], venue_positions={})
    assert plan.action == "start"
    # The crash is still REPORTED — starting is not the same as pretending it did not happen.
    assert plan.reason == "unclean_exit_but_flat"


# ── unknown exposure is an OPERATOR decision, never an automatic flatten ─────────────────────

def test_a_venue_position_we_never_recorded_REFUSES_rather_than_auto_flattening():
    """Flattening moves real money against a basis we do not own. This is the reconciler's
    whitelist discipline: unknown exposure halts, it does not get traded away."""
    plan = assess_recovery(_state(inventory={}), venue_orders=[],
                           venue_positions={"KXOTHER": Decimal("7")})
    assert plan.action == "refuse"
    assert plan.reason == "unknown_position"
    assert plan.unknown == (("KXOTHER", Decimal("7")),)


def test_a_venue_position_LARGER_than_we_recorded_is_unknown_exposure():
    plan = assess_recovery(_state(inventory={"KXA": Decimal("3")}), venue_orders=[],
                           venue_positions={"KXA": Decimal("10")})
    assert plan.action == "refuse"
    assert plan.reason == "unknown_position"


def test_a_venue_position_SMALLER_than_we_recorded_is_not_unknown_exposure():
    """Settlement and partial fills shrink positions constantly. Refusing on `venue < known` would
    be a false-positive storm — the same call `reconcile.py` makes, for the same reason."""
    plan = assess_recovery(_state(clean_exit=False, exit_status=None,
                                  inventory={"KXA": Decimal("10")}),
                           venue_orders=[], venue_positions={"KXA": Decimal("3")})
    assert plan.action == "recover"
    assert plan.flatten == (("KXA", Decimal("3"))    ,)


def test_a_venue_position_with_the_OPPOSITE_SIGN_of_the_record_is_unknown_exposure():
    """⛔ The magnitude-only comparison read venue −12 against a recorded +12 as 'attributable'
    and planned to flatten it — but the record describes the OPPOSITE position, so the basis is
    fiction. Sign disagreement is exactly as unknown as a position we never recorded: it is the
    belief-inverted-against-the-venue corruption shape, reaching the recovery tool."""
    plan = assess_recovery(_state(inventory={"KXA": Decimal("12")}), venue_orders=[],
                           venue_positions={"KXA": Decimal("-12")})
    assert plan.action == "refuse"
    assert plan.reason == "unknown_position"
    assert plan.unknown == (("KXA", Decimal("-12")),)


def test_orders_are_still_listed_for_cancellation_even_when_the_plan_REFUSES():
    """A refusal must not leave live orders resting while an operator sleeps. Cancelling is
    always safe; the refusal is about STARTING, not about cleanup."""
    plan = assess_recovery(_state(), venue_orders=[{"order_id": "oid-9", "ticker": "KXA"}],
                           venue_positions={"KXOTHER": Decimal("7")})
    assert plan.action == "refuse"
    assert plan.cancel_order_ids == ("oid-9",)


def test_sub_resolution_venue_dust_is_not_a_position():
    """A ~1e-13 residue read as real size is this repo's recurring compare-to-zero bug (the
    crossed-book float dust). Here it would refuse to start, forever, over nothing."""
    plan = assess_recovery(_state(), venue_orders=[],
                           venue_positions={"KXA": Decimal("0.00000001")})
    assert plan.action == "start"


# ── the durable loss cap gates the restart ───────────────────────────────────────────────────

def test_a_breached_durable_loss_cap_refuses_the_next_start():
    """Otherwise a crash-restart loop resets the budget on every crash and the cap bounds nothing."""
    plan = assess_recovery(_state(realized_pnl=Decimal("-6"), loss_cap=Decimal("5")),
                           venue_orders=[], venue_positions={})
    assert plan.action == "refuse"
    assert plan.reason == "loss_cap"


def test_cleanup_still_happens_when_the_loss_cap_refuses():
    plan = assess_recovery(_state(realized_pnl=Decimal("-6"), loss_cap=Decimal("5")),
                           venue_orders=[{"order_id": "oid-9", "ticker": "KXA"}],
                           venue_positions={})
    assert plan.cancel_order_ids == ("oid-9",)


def test_an_unbreached_cap_does_not_refuse():
    assert assess_recovery(_state(realized_pnl=Decimal("-1"), loss_cap=Decimal("5")),
                           venue_orders=[], venue_positions={}).action == "start"


# ── no prior state at all ────────────────────────────────────────────────────────────────────

def test_no_state_file_and_a_flat_venue_may_start():
    assert assess_recovery(None, venue_orders=[], venue_positions={}).action == "start"


def test_no_state_file_but_the_venue_holds_something_REFUSES():
    """First-ever run against an account that already holds inventory. The maker's loss cap and
    flatten arithmetic are both wrong against an inherited basis, so this is
    the case that must not silently proceed."""
    plan = assess_recovery(None, venue_orders=[], venue_positions={"KXA": Decimal("4")})
    assert plan.action == "refuse"
    assert plan.reason == "unknown_position"


def test_no_state_file_and_an_unreadable_venue_REFUSES():
    assert assess_recovery(None, venue_orders=None, venue_positions=None).action == "refuse"


# ── a corrupt state file is not a clean slate ────────────────────────────────────────────────

def test_a_corrupt_state_file_refuses_to_start(tmp_path):
    p = tmp_path / "maker_state.json"
    p.write_text("{trunca")
    plan = maker_state.assess_recovery_from_disk(str(p), venue_orders=[], venue_positions={})
    assert plan.action == "refuse"
    assert plan.reason == "state_corrupt"


def test_assess_from_disk_matches_the_pure_function_on_a_good_file(tmp_path):
    s = maker_state.MakerStateStore(path=str(tmp_path / "maker_state.json"))
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.end_run("clean")
    plan = maker_state.assess_recovery_from_disk(s.path, venue_orders=[], venue_positions={})
    assert plan.action == "start"


def test_the_plan_renders_a_human_report():
    plan = assess_recovery(_state(clean_exit=False, exit_status=None,
                                  inventory={"KXA": Decimal("3")}),
                           venue_orders=[{"order_id": "oid-9", "ticker": "KXA"}],
                           venue_positions={"KXA": Decimal("3")})
    text = plan.report()
    assert "oid-9" in text and "KXA" in text and "recover" in text.lower()
