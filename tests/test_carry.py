"""Pins for CONTINUOUS OPERATION — the --carry mechanism.

Why it exists: the recovery assessor correctly classifies a clean-exit carried position as
`attributable` → plan "recover: flatten" — so a run that deliberately holds inventory across
a restart gets refused or flattened, paying the rejoin tax (measured at several times the
round-trip cost) that the carry exists to kill. `apply_carry` converts EXACTLY the
declared-and-verified case and nothing else.

Every pin here is a refusal, because a wrong carry is a wrong BASIS on real money.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from bot.core.maker_state import RecoveryPlan, apply_carry
from scripts.poly_live_mm import parse_carry

D = Decimal


def _recover(flatten, cancel=()):
    return RecoveryPlan("recover", "live_venue_state", "cleanup",
                        cancel_order_ids=tuple(cancel), flatten=tuple(flatten))


class TestApplyCarry:
    VENUE = {"dem": D("170"), "hormuz": D("-10")}

    def test_exact_declarations_convert_recover_to_start(self):
        plan = apply_carry(_recover([("dem", D("170")), ("hormuz", D("-10"))]),
                           {"dem": D("170"), "hormuz": D("-10")}, self.VENUE)
        assert plan.action == "start" and plan.reason == "carried_inventory"
        assert "REDUCE-ONLY" in plan.report() or "REDUCE-ONLY" in plan.detail, (
            "the at-cap consequence must be stated where the operator reads it")

    def test_a_quantity_off_by_ONE_refuses_everything(self):
        """A stale declaration means the operator is describing a venue that no longer
        exists — every carry on that line is suspect, so the whole start refuses."""
        plan = apply_carry(_recover([("dem", D("170"))]), {"dem": D("169")},
                           {"dem": D("170")})
        assert plan.action == "refuse" and plan.reason == "carry_mismatch"

    def test_a_sign_flip_refuses(self):
        plan = apply_carry(_recover([("hormuz", D("-10"))]), {"hormuz": D("10")},
                           {"hormuz": D("-10")})
        assert plan.action == "refuse"

    def test_an_UNDECLARED_live_book_keeps_its_recover_verdict(self):
        """A carry is explicit, per book: declaring dem must not silently bless hormuz."""
        plan = apply_carry(_recover([("dem", D("170")), ("hormuz", D("-10"))]),
                           {"dem": D("170")}, self.VENUE)
        assert plan.action == "recover"
        assert "hormuz" in plan.detail and "UNDECLARED" in plan.detail

    def test_a_resting_order_blocks_conversion_entirely(self):
        """A carry declares POSITIONS, never orders — any stray order keeps the plan."""
        plan = apply_carry(_recover([("dem", D("170"))], cancel=("OID1",)),
                           {"dem": D("170")}, {"dem": D("170")})
        assert plan.action == "recover"

    def test_an_unknown_position_refusal_is_NEVER_converted(self):
        """No durable record = no basis to carry; the operator's number cannot substitute
        for a record we do not hold."""
        refusal = RecoveryPlan("refuse", "unknown_position", "no record")
        assert apply_carry(refusal, {"dem": D("170")}, {"dem": D("170")}) is refusal

    def test_a_declared_book_absent_from_the_plans_flatten_refuses(self):
        plan = apply_carry(_recover([("dem", D("170"))]),
                           {"dem": D("170"), "ghost": D("5")},
                           {"dem": D("170"), "ghost": D("5")})
        assert plan.action == "refuse"

    def test_start_and_refuse_plans_pass_through_untouched(self):
        start = RecoveryPlan("start", "clean", "flat")
        assert apply_carry(start, {"dem": D("170")}, {"dem": D("170")}) is start
        assert apply_carry(_recover([("dem", D("170"))]), {}, self.VENUE).action == "recover"


class TestReviewRequestedPins:
    """The three properties a money-path review of the carry path asks for, in priority order."""

    def test_a_short_seeded_at_the_YES_space_basis_marks_SMALL_not_catastrophic(self):
        """⛔ The one number whose error is silent and total. A short built by selling YES at
        0.13 marks a few cents against an ask of 0.135; had the basis been the NO-space
        complement of an 0.87 sale, the same call returns a mark two orders of magnitude
        larger and the tripwire arms on cycle 1."""
        from bot.poly_us.maker import mark_pnl
        good = mark_pnl(D("-10"), D("0.13"), bid=D("0.12"), ask=D("0.135"))
        assert good is not None and D("-0.10") < good < D("0"), (
            f"yes-space basis must mark small, got {good}")
        bad = mark_pnl(D("-10"), D("0.868"), bid=D("0.12"), ask=D("0.135"))
        assert bad is not None and bad > D("5"), (
            f"the trap basis must be OBVIOUS in the mark (got {bad}) — this pin documents "
            f"the failure shape the dual-source runbook line exists to prevent; note the "
            f"trap marks as a huge GAIN for a short, the flattering direction, which is "
            f"why the tripwire alone cannot catch it and the provenance line must")

    def test_begin_run_seeds_the_VERIFIED_view_never_the_prior_copy(self, tmp_path):
        """⛔ DELIBERATELY SUPERSEDES the 'carries the prior record's inventory' pin — the
        protection it gave is PRESERVED and strengthened, not dropped. The old wholesale copy
        is how a dead run's stale belief rode into a relaunch's record with no venue standing
        behind it. begin_run now takes the
        RECOVERY-VERIFIED view as a required parameter: a carry-start records exactly the
        declared quantities — never a flat record over live contracts, the rule that keeps a
        real position from becoming invisible, now sourced from the venue-verified
        declaration instead of an unverified copy;
        a clean start records {} and the prior map does NOT ride along."""
        from bot.core.maker_state import MakerStateStore
        path = str(tmp_path / "state.json")
        s1 = MakerStateStore(path)
        s1.begin_run("run-1", mode="real", loss_cap=D("10"), tickers=["dem"],
                     inventory={})
        s1.set_inventory({"dem": D("170"), "hormuz": D("-10")})
        s1.end_run("clean")

        # Carry-start: the verified declaration seeds the record — never {} over a carry.
        s2 = MakerStateStore(path)
        s2.begin_run("run-2", mode="real", loss_cap=D("10"), tickers=["dem", "hormuz"],
                     inventory={"dem": D("170"), "hormuz": D("-10")})
        inv = s2.snapshot().inventory
        assert inv.get("dem") == D("170") and inv.get("hormuz") == D("-10"), (
            f"a carry-start must record the declared quantities, got {inv} — a flat "
            f"record over live contracts is the belief-divergence class")
        s2.end_run("clean")

        # Clean start: {} is the record, and the prior 170/-10 must NOT be copied in.
        s3 = MakerStateStore(path)
        s3.begin_run("run-3", mode="real", loss_cap=D("10"), tickers=["dem"],
                     inventory={})
        assert s3.snapshot().inventory == {}, (
            f"a clean start seeds empty — the prior map riding along is the retired "
            f"wholesale copy (got {s3.snapshot().inventory})")

    def test_begin_run_requires_the_inventory_parameter(self, tmp_path):
        """Keyword-only and REQUIRED — no caller may silently fall back to the old copy."""
        from bot.core.maker_state import MakerStateStore
        s1 = MakerStateStore(str(tmp_path / "state.json"))
        try:
            s1.begin_run("run-1", mode="real", loss_cap=D("10"), tickers=["dem"])  # noqa — deliberately missing
        except TypeError:
            return
        raise AssertionError("begin_run without inventory= must be a TypeError")

    def test_begin_run_still_carries_the_loss_ledger(self, tmp_path):
        """The carry path touches ONLY inventory — realized_pnl remains the lifetime ratchet."""
        from bot.core.maker_state import MakerStateStore
        path = str(tmp_path / "state.json")
        s1 = MakerStateStore(path)
        s1.begin_run("run-1", mode="real", loss_cap=D("10"), tickers=["dem"],
                     inventory={})
        s1.add_realized(D("-2.50"))
        s1.end_run("clean")
        s2 = MakerStateStore(path)
        s2.begin_run("run-2", mode="real", loss_cap=D("10"), tickers=["dem"],
                     inventory={})
        assert s2.snapshot().realized_pnl == D("-2.50")

    def test_quantity_match_is_EXACT_across_decimal_representations(self):
        """Review priority 3: venue position_fp can be fractional-formatted. '170.0' == 170
        must pass (same number); 169.99 must refuse."""
        plan = apply_carry(_recover([("dem", D("170"))]), {"dem": D("170")},
                           {"dem": "170.0"})
        assert plan.action == "start"
        plan = apply_carry(_recover([("dem", D("170"))]), {"dem": D("170")},
                           {"dem": "169.99"})
        assert plan.action == "refuse"


class TestParseCarry:
    SLUGS = ["dem", "hormuz"]

    def test_a_valid_line_parses_longs_and_shorts(self):
        got = parse_carry(["dem:170@0.8474", "hormuz:-10@0.13"], self.SLUGS)
        assert got == {"dem": (D("170"), D("0.8474")), "hormuz": (D("-10"), D("0.13"))}

    def test_every_malformation_refuses(self):
        for bad in (["dem:170"], ["dem@0.5"], ["dem:x@0.5"], ["dem:170@x"],
                    ["dem:0@0.5"], ["dem:170@1.5"], ["dem:170@0"], ["nope:1@0.5"],
                    ["dem:170@0.5", "dem:1@0.5"]):
            with pytest.raises(SystemExit):
                parse_carry(bad, self.SLUGS)

    def test_the_complement_collateral_trap_refuses_at_the_boundary(self):
        """The venue UI shows a SHORT's cost as complement collateral (here 0.868/contract);
        the engine's basis is the yes-space SALE price (0.13). The parse cannot tell 0.868
        from a legitimate basis numerically — but a value ≥1 (a total-dollars paste like
        8.68) refuses, and the help text carries the semantic warning. This pin documents the
        residual: 0.868 WOULD parse, and only a dual-source print of the basis at
        registration catches it."""
        with pytest.raises(SystemExit):
            parse_carry(["hormuz:-10@8.68"], self.SLUGS)
        got = parse_carry(["hormuz:-10@0.868"], self.SLUGS)   # parses — documented residual
        assert got["hormuz"][1] == D("0.868")
