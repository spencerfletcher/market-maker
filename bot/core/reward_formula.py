"""The venue's DOCUMENTED liquidity-reward formula — pure arithmetic, Decimal in/out.

Source: https://docs.polymarket.us/incentives/liquidity (read 2026-09-07, re-fetched 2026-09-08).
The venue's own words — this block is the OWNER TEXT for every rule below:

    "Score = Discount Factor ^ (ticks from best price) × Order Size"
    "Each side of the book is scored independently; the spread between your bid and offer
     doesn't matter."
    "Every second, a random snapshot of the order book is taken."
    "Each snapshot is normalized; the bid side and ask side are each independently normalized
     to 1.0 per snapshot"

    THE WALK (`qualifying_range`, shipped 2026-09-08):
    "The exchange walks from the best price outward, accumulating orders until Target Size is
     reached." "All orders within that range score; orders beyond it do not." "If Target Size
     is reached before your price level, your order will not score, regardless of how close it
     is"

    THE PERIODS (non-overlapping by definition):
    "Early / Pre-game (pre-day): From market listing until 6 hours before the event"
    "Day-of / Pre-game: From 6 hours before until the event starts"
    "Live: From event start until settlement"

    PAYMENT:
    "Rewards are calculated within 5 business days of each time period ending and credited to
     your account within 2 business days" · "Rewards under <n> are not paid out."

so every second is equally weighted PROVIDED our order is INSIDE THE WALKED RANGE — ⛔ not
"the side's aggregate reached target size", which is the level-inclusive test this module carried
until 2026-09-08 and which the walk replaces — and payout is purely proportional to
share of total score — no per-user cap, no two-sided requirement, no max-spread rule. Each
program's pool is split pro-rata by score across the markets in that program.

TWO NAMED ASSUMPTIONS ride every figure this module produces. Neither is a doc quote; both are
stated so a reader can re-derive without them.

1. **THE DIVISOR.** The doc does not say whether a snapshot normalizes per MARKET-side or per
   PROGRAM-side. ⛔ One of the two is already ELIMINATED, not open: `pool ÷ (2 × n_markets)` was
   RETRACTED against the venue's own PAID ledger — 4 of 16 rows implied a share ABOVE 1.0, up to
   7.28× (the private design notes § reward pool per PROGRAM; reward_model_reconciliation_2026-08-17 §2),
   and `pool ÷ 2` is its named replacement, an UPPER BOUND rather than a settled value. So
   `per_program_side` (the market's listed pool, halved per side) is the DEFAULT everywhere a
   single figure must be written, and `per_market_side` survives only as the hypothesis
   `scripts/poly_reward_formula_validate.py` scores against the paid ledger.
2. **THE PERIOD LENGTH.** `expected_side_usd` divides by the period INSTANCE's seconds, which the
   incentives sweep does not carry (sports rows have a blank `end`). The model, from the doc's own
   period definitions: `day_of` = exactly 6 h · `live` = event start → settlement, approximated by
   the last cycle any tape holds for the book (`live_len:approx`) · `early` = listing
   (`created_at`) → event start − 6 h · `daily` = one ET calendar day. A period whose bound is
   unreadable is UNMEASURED, never defaulted (`scripts/poly_reward_formula_read.py`).

⚠️ MODELED, never measured: nothing in this module reads what the venue paid. The empirical
paid-rate model (`poly_capital_rewards.modeled_class_rewards_from_days`) is the CROSS-CHECK and is
printed beside these figures, never summed with them.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING
from typing import Optional, Sequence

#: The two divisor hypotheses, by name. `per_market_side` normalizes each MARKET's side to 1.0 per
#: second (divisor = 2 × seconds × active markets); `per_program_side` normalizes each PROGRAM's
#: side once (divisor = 2 × seconds). They differ by exactly `active_markets`.
#: ⚠️ NEITHER IS ADOPTED and this tuple is UNCHANGED by the 2026-09-08 walk. Note only that the
#: doc normalizes PER SNAPSHOT ("each snapshot is normalized … to 1.0"), which makes the natural
#: program unit the (market, side, snapshot) — the hypothesis `poly_reward_divisor_calib` tests
#: against paid money; nothing here presumes it.
DIVISOR_PER_MARKET_SIDE = "per_market_side"
DIVISOR_PER_PROGRAM_SIDE = "per_program_side"
DIVISORS = (DIVISOR_PER_MARKET_SIDE, DIVISOR_PER_PROGRAM_SIDE)

#: The two sides, spelled as the quote tape spells them.
SIDE_BID = "bid"
SIDE_ASK = "ask"

_ZERO = Decimal(0)


def ticks_off(our_price: Decimal, best_price: Decimal, tick: Decimal, side: str) -> int:
    """Ticks our resting price sits BEHIND the side's best. 0 = at the best OR improving on it.

    A bid below the best bid, or an ask above the best ask, is behind by that many ticks; the
    other direction is a new best and scores the full `discount^0`. An off-grid price rounds
    AWAY from the touch (ROUND_CEILING on the tick count) — never toward it, which would
    over-credit a price the venue would bin further out.
    """
    if tick <= 0:
        raise ValueError("tick must be positive")
    if side == SIDE_BID:
        behind = best_price - our_price
    elif side == SIDE_ASK:
        behind = our_price - best_price
    else:
        raise ValueError(f"side must be {SIDE_BID!r} or {SIDE_ASK!r}, got {side!r}")
    if behind <= 0:
        return 0
    return int((behind / tick).to_integral_value(rounding=ROUND_CEILING))


def qualifying_range(ladder: Sequence[tuple[int, Decimal]], target_size: Decimal, *,
                     best: Decimal, tick: Decimal, our_price: Decimal, our_size: Decimal,
                     side: str) -> tuple[Optional[int], bool]:
    """The venue's target-size WALK over a cumulative ladder → (last qualifying level, reached).

    "The exchange walks from the best price outward, accumulating orders until Target Size is
    reached. All orders within that range score; orders beyond it do not."

    `ladder` is `poly_park_scan.cum_ladder`'s `[(ticks_off, cum_size), …]` from the touch
    outward — the venue's whole book, so OUR OWN resting size is already in the cum at our own
    level when the sampler saw it. When our level is ABSENT from the ladder (the sampler
    truncated at target, or sampled before we rested) our order is inserted there at `our_size`:
    the venue walks the book with our order in it, and a walk that leaves it out understates the
    accumulation at and past our level.

    Returns `(None, False)` for an empty ladder — UNREAD, never "the target was unmet". The
    caller's eligibility rule is the doc's own sentence: our order scores unless the walk reached
    `target_size` STRICTLY BEFORE our price level.
    """
    if not ladder:
        return None, False
    our_ticks = ticks_off(our_price, best, tick, side)
    increments: dict[int, Decimal] = {}
    previous = _ZERO
    for level, cum in sorted((int(n), Decimal(c)) for n, c in ladder):
        increments[level] = max(_ZERO, cum - previous)
        previous = cum
    if our_ticks not in increments:
        increments[our_ticks] = our_size
    cumulative, last = _ZERO, None
    for level in sorted(increments):
        cumulative += increments[level]
        last = level
        if target_size > 0 and cumulative >= target_size:
            return level, True
    return last, False


def side_score(discount: Decimal, ticks: int, size: Decimal) -> Decimal:
    """`DiscountFactor^(ticks from best price) × OrderSize` — the doc's per-second side score."""
    if ticks < 0:
        raise ValueError("ticks must be >= 0")
    return discount ** ticks * size


def side_share(our_score: Decimal, competitor_base: Decimal) -> Decimal:
    """Our share of one normalized side-snapshot: `S / (S + base)`.

    THE DENOMINATOR IS THE WALKED RANGE, OURS INCLUDED. "Each snapshot is normalized; the bid
    side and ask side are each independently normalized to 1.0 per snapshot" — so the shares of
    every order INSIDE the range sum to exactly 1.0 per side per snapshot, and an order the walk
    excluded (`qualifying_range`) contributes to neither numerator nor denominator.
    `competitor_base` is the discount-weighted resting size of EVERYONE ELSE scoring on that side
    (`poly_park_scan.side_base`, netted of our own score by the caller — the sampler truncates its
    ladder at target size, so that base is already range-scoped where the sampler could see it).
    Pro-rata puts our own score in the denominator, so scaling is sub-linear by construction.
    A dead side (our score 0 and no base) shares nothing.
    """
    total = our_score + competitor_base
    return _ZERO if total <= 0 else our_score / total


def expected_side_usd(pool_usd: Decimal, period_seconds: Decimal, seconds_present: Decimal,
                      share: Decimal, target_met: bool, divisor: str,
                      active_markets: int) -> Decimal:
    """Expected USD from ONE side over `seconds_present` of a program period.

    `pool × (seconds_present × share) ÷ divisor`, where the divisor is the period's total
    normalized side-seconds under the named hypothesis:
      · `per_market_side`  → 2 × period_seconds × active_markets
      · `per_program_side` → 2 × period_seconds

    ⛔ Returns 0 when the side's TARGET SIZE was not met: an unmet target earns nothing at all,
    which is a different statement from earning a small share.
    """
    if divisor not in DIVISORS:
        raise ValueError(f"divisor must be one of {DIVISORS}, got {divisor!r}")
    if not target_met:
        return _ZERO
    if period_seconds <= 0 or active_markets < 1:
        return _ZERO
    sides = Decimal(2)
    total = (sides * period_seconds * Decimal(active_markets)
             if divisor == DIVISOR_PER_MARKET_SIDE else sides * period_seconds)
    return pool_usd * (seconds_present * share) / total
