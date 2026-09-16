"""The venue's DOCUMENTED liquidity-reward formula — arithmetic pins.

Source of every number below: https://docs.polymarket.us/incentives/liquidity (read 2026-09-07),
`score = DiscountFactor^(ticks from best price) × OrderSize`, per side, each snapshot normalized
to 1.0 per side. ⛔ Nothing here is a measurement.
"""
from decimal import Decimal

import pytest

from bot.core import reward_formula as rf


class TestTheDocumentedScore:
    """⛔ MUTANT: change the exponent's base or its sign — every point below goes RED."""

    def test_the_score_at_three_points_including_the_doc_worked_case(self):
        # AT the best: the discount never applies, so the score IS the size.
        assert rf.side_score(Decimal("0.30"), 0, Decimal(10)) == Decimal(10)
        # ⛔ ONE TICK OFF AT d=0.30 IS 30% OF THE SIZE — the doc's own reading of the exponent.
        assert rf.side_score(Decimal("0.30"), 1, Decimal(10)) == Decimal(3)
        # Two ticks compounds: 0.30² × 10.
        assert rf.side_score(Decimal("0.30"), 2, Decimal(10)) == Decimal("0.9")

    def test_a_negative_tick_count_is_refused_never_scored(self):
        """An improving price is 0 ticks off, and `ticks_off` says so — a negative exponent here
        would INFLATE the score above the touch's, which the doc's formula never does."""
        with pytest.raises(ValueError):
            rf.side_score(Decimal("0.30"), -1, Decimal(10))

    def test_the_share_is_pro_rata_with_our_own_score_in_the_denominator(self):
        assert rf.side_share(Decimal(10), Decimal(90)) == Decimal("0.1")
        # A dead side shares nothing rather than dividing by zero.
        assert rf.side_share(Decimal(0), Decimal(0)) == Decimal(0)


class TestTicksOff:
    """⛔ MUTANT: flip the sign on either side — the improving case reads as behind (or the
    behind case as at-the-touch) and both halves below go RED."""

    def test_the_BID_side_counts_ticks_BELOW_the_best_and_zero_above_it(self):
        tick = Decimal("0.01")
        assert rf.ticks_off(Decimal("0.49"), Decimal("0.49"), tick, rf.SIDE_BID) == 0
        assert rf.ticks_off(Decimal("0.47"), Decimal("0.49"), tick, rf.SIDE_BID) == 2
        # IMPROVING on the best bid is a new best — 0, never a negative exponent.
        assert rf.ticks_off(Decimal("0.51"), Decimal("0.49"), tick, rf.SIDE_BID) == 0

    def test_the_ASK_side_counts_ticks_ABOVE_the_best_and_zero_below_it(self):
        tick = Decimal("0.01")
        assert rf.ticks_off(Decimal("0.51"), Decimal("0.51"), tick, rf.SIDE_ASK) == 0
        assert rf.ticks_off(Decimal("0.53"), Decimal("0.51"), tick, rf.SIDE_ASK) == 2
        assert rf.ticks_off(Decimal("0.49"), Decimal("0.51"), tick, rf.SIDE_ASK) == 0

    def test_an_OFF_GRID_price_rounds_AWAY_from_the_touch(self):
        """Never toward it: rounding a 1.5-tick price to 1 would over-credit a price the venue
        bins further out."""
        assert rf.ticks_off(Decimal("0.475"), Decimal("0.49"), Decimal("0.01"),
                            rf.SIDE_BID) == 2


class TestTheDivisorHypotheses:
    """⛔ THE AMBIGUITY THE DOC DOES NOT RESOLVE, carried as two named divisors. Neither is
    preferred here; `poly_reward_formula_validate` is the read that decides."""

    #: <n> pool, a 7,200 s period, 3,600 s present at a 10% side share, 5 markets on the program.
    TERMS = dict(pool_usd=Decimal(500), period_seconds=Decimal(7200),
                 seconds_present=Decimal(3600), share=Decimal("0.1"), target_met=True,
                 active_markets=5)

    def test_both_divisors_on_ONE_worked_example(self):
        # per_market_side: 500 × (3600 × 0.1) ÷ (2 × 7200 × 5) = 500 × 360 ÷ 72,000.
        assert rf.expected_side_usd(divisor=rf.DIVISOR_PER_MARKET_SIDE,
                                    **self.TERMS) == Decimal("2.5")
        # per_program_side: the same numerator over (2 × 7200) — exactly `active_markets` times
        # larger, which is the whole disagreement between the two hypotheses.
        assert rf.expected_side_usd(divisor=rf.DIVISOR_PER_PROGRAM_SIDE,
                                    **self.TERMS) == Decimal("12.5")

    def test_an_UNMET_TARGET_SIZE_earns_ZERO_under_either_divisor(self):
        """⛔ THE DECISION LINE. The doc pays a side only while the side's aggregate resting size
        meets the program's target; below it the side earns nothing at all, which is not the same
        as earning a small share.

        ⛔ MUTANT: drop the `target_met` gate — both assertions go RED."""
        for divisor in rf.DIVISORS:
            terms = dict(self.TERMS, target_met=False, divisor=divisor)
            assert rf.expected_side_usd(**terms) == Decimal(0)

    def test_an_unknown_divisor_is_refused_never_silently_defaulted(self):
        with pytest.raises(ValueError):
            rf.expected_side_usd(divisor="per_snapshot", **self.TERMS)


class TestTheTargetSizeWALK:
    """The venue's own rule, shipped 2026-09-08: "The exchange walks from the best price outward,
    accumulating orders until Target Size is reached. All orders within that range score; orders
    beyond it do not. If Target Size is reached before your price level, your order will not
    score, regardless of how close it is."
    """

    @staticmethod
    def _brute(ladder, target, our_ticks, our_size):
        """The doc's sentence, spelled out independently of the implementation: increments per
        level, our own order inserted at our level when the ladder has none there, walk outward."""
        increments, previous = {}, Decimal(0)
        for n, cum in sorted(ladder):
            increments[n] = max(Decimal(0), cum - previous)
            previous = cum
        increments.setdefault(our_ticks, our_size)
        before = sum((size for n, size in increments.items() if n < our_ticks), Decimal(0))
        cumulative, last, reached = Decimal(0), None, False
        for n in sorted(increments):
            cumulative += increments[n]
            last = n
            if target > 0 and cumulative >= target:
                reached = True
                break
        return before < target, last, reached

    def test_the_walk_matches_the_brute_force_rule_on_240_random_books(self):
        """⛔ SEEDED TRUTH SIMULATION. Eligibility is exactly "cumulative size BEFORE our level <
        target"; `last_qualifying_level`/`reached` are the range the doc says scores.

        ⛔ MUTANT: drop our own size from the accumulation (`increments[our_ticks] = our_size`)
        — the level/reached assertion goes RED on the books whose ladder stops short of our
        level.
        ⛔ MUTANT: walk with `<` instead of `<=` before our level (score an order the walk had
        already closed) — the eligibility assertion goes RED."""
        import random
        rng = random.Random(20260908)
        tick, best = Decimal("0.01"), Decimal("0.50")
        seen_missing_level = 0
        for _case in range(240):
            levels = sorted(rng.sample(range(0, 9), rng.randint(1, 5)))
            cumulative, ladder = Decimal(0), []
            for n in levels:
                cumulative += Decimal(rng.randint(1, 60))
                ladder.append((n, cumulative))
            target = Decimal(rng.randint(1, 200))
            our_ticks = rng.randint(0, 8)
            our_size = Decimal(rng.randint(1, 40))
            our_price = best - tick * our_ticks
            eligible, last, reached = self._brute(tuple(ladder), target, our_ticks, our_size)
            got_last, got_reached = rf.qualifying_range(
                tuple(ladder), target, best=best, tick=tick, our_price=our_price,
                our_size=our_size, side=rf.SIDE_BID)
            assert (got_last, got_reached) == (last, reached), (ladder, target, our_ticks)
            assert (not (got_reached and got_last < our_ticks)) is eligible, (
                ladder, target, our_ticks)
            seen_missing_level += our_ticks not in dict(ladder)
        assert seen_missing_level >= 20, "the our-size insertion branch has to be exercised"

    def test_an_ABSENT_ladder_is_UNREAD_never_a_met_or_unmet_target(self):
        """⛔ (None, False) — the caller labels it `no_ladder`/`unmeasurable`. Reading it as
        "target unmet" is the pre-09-08 test the walk replaces."""
        assert rf.qualifying_range((), Decimal(100), best=Decimal("0.50"), tick=Decimal("0.01"),
                                   our_price=Decimal("0.49"), our_size=Decimal(10),
                                   side=rf.SIDE_BID) == (None, False)

    def test_the_shares_INSIDE_the_range_sum_to_exactly_one_per_side(self):
        """"Each snapshot is normalized; the bid side and ask side are each independently
        normalized to 1.0 per snapshot" — so the scoring orders' shares sum to 1, and an order
        the walk excluded is in neither the numerator nor the denominator."""
        d = Decimal("0.30")
        # Three scoring orders at 0, 1 and 2 ticks off; a fourth sits beyond the walk.
        inside = [rf.side_score(d, n, size) for n, size in ((0, Decimal(40)), (1, Decimal(30)),
                                                            (2, Decimal(20)))]
        total = sum(inside, Decimal(0))
        shares = [rf.side_share(score, total - score) for score in inside]
        # (28-significant-digit division, so equality is to the Decimal context's last place.)
        assert abs(sum(shares, Decimal(0)) - Decimal(1)) < Decimal("1e-27")
        assert rf.side_share(inside[0], total - inside[0]) > shares[2], "closer scores more"
