"""
bot/core/money.py
─────────────────
Exact decimal arithmetic for money.

WHY THIS EXISTS. Float error at this system's magnitudes (prices in [0,1], small money amounts) is
~1e-13 — orders of magnitude below the smallest meaningful unit (the centicent, 1e-4). It is therefore
harmless *except* when AMPLIFIED across a threshold by a `ceil`/`floor` or a compare-to-zero. Both
production precision bugs were exactly that:

  • the fee bug — `0.07*0.5*0.5*1e4 == 175.00000000000003`, so a bare ceil() charged a whole extra
    centicent
  • the crossed-book dust — a removed level left qty ~1e-13, and `qty > 0` kept the ghost, producing
    phantom fat edges on a majority of the candidates sampled at fire time

Both were patched with a hand-placed `round(x, 6)` before the amplifying op. That works, but it makes
correctness depend on every future author *remembering* the guard — which is precisely how both bugs got
in. These helpers make the guard impossible to forget: use `floor_to`/`ceil_to` and the rounding is
structural, not remembered.

WHY DECIMAL IS THE NATURAL TYPE HERE. Both venues speak decimal STRINGS on the wire
(`"yes_price_dollars": "0.4000"`, `count_fp: "1.00"`, Poly `{"value": "0.42"}`) and we send strings back
(`f"{p:.4f}"`). So `Decimal(raw_string)` is exact end-to-end and float is the lossy intermediate — there
is no lossy boundary to fight.

CONTEXT NOTE: this module never mutates the global decimal context (that would be a process-wide side
effect). Every result that matters is `quantize`d explicitly, so nothing here depends on ambient
precision. The default 28 significant digits is ample for division.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

__all__ = ["CENT", "CENTICENT", "D", "from_float", "parse_wire", "complement", "floor_to", "ceil_to", "is_zero"]

CENT = Decimal("0.01")          # Poly fee grid (rounded on the ORDER TOTAL)
CENTICENT = Decimal("0.0001")   # Kalshi fee grid; the smallest unit either venue quotes


def D(value: str | int | Decimal) -> Decimal:
    """Exact Decimal from a string/int/Decimal — the ONLY safe constructor.

    **Refuses `float` deliberately.** `Decimal(0.1)` is `0.1000000000000000055511151231257827…` — it
    imports the very error this module exists to remove, silently. Wire values arrive as strings, so the
    exact path is always available; if you genuinely hold a float (an SDK already parsed it), say so
    explicitly with `from_float`."""
    if isinstance(value, float):
        raise TypeError(
            "D() refuses float — Decimal(0.1) silently imports binary error. Parse the venue's raw "
            "string instead, or call from_float() if the value is genuinely already a float.")
    return Decimal(value)


def from_float(value: float) -> Decimal:
    """Explicit, *named* lossy boundary: float → Decimal via `repr` (shortest round-tripping form), so
    0.1 becomes Decimal('0.1') rather than the full binary expansion.

    Use ONLY where a float genuinely arrives from outside (an SDK that already parsed the wire). It is
    deliberately verbose so it stands out in review — a `from_float` on a value that came from a string
    is a bug at the parse site, not here."""
    return Decimal(repr(float(value)))


def parse_wire(value: str | int | float | Decimal) -> Decimal:
    """THE ingestion boundary: a value as the venue sent it → exact Decimal.

    Both venues quote decimal strings (`"0.4000"`, `"1.00"`, `{"value": "0.42"}`), for which this is
    exact. A JSON *number* is accepted too and routed through `from_float`'s shortest-round-trip, so a
    field that arrives unquoted still recovers its intended decimal rather than a binary expansion.

    Prefer this at every parse site over `float(...)`: parsing to float first and converting later
    throws away exactness *before* we get a chance to keep it."""
    if isinstance(value, float):
        return from_float(value)
    return Decimal(value)


def complement(price: float) -> float:
    """Exact `1 − price` — the opposite side of a binary market — at the float boundary.

    Both venues price complementary outcomes, so this is the single most repeated money operation in
    the codebase: a yes bid at p IS a no offer at 1−p. In float, `1.0 - 0.55` is 0.44999999999999996,
    so every site used to carry a hand-placed `round(1.0 - p, 6)`. Exact subtraction makes the guard
    structural, and on the wire grid (<=4 dp) it is an exact involution — `complement(complement(p))
    == p`, which does NOT hold for arbitrary floats (~31% of random doubles fail) — which matters
    because the two complements sit on opposite sides of the same trade and are compared to each other
    (one side walks levels with `ask <= limit`, the other with `px >= 1 - limit`).

    Float in/out deliberately — this is a strangler-fig boundary and callers still hold floats. Lossless
    for wire values; a computed (already-float) input can only be as exact as the float it came in as."""
    return float(D(1) - from_float(price))


def floor_to(value: Decimal, step: Decimal = CENTICENT) -> Decimal:
    """Largest multiple of `step` that is <= value. Exact — no pre-round guard needed.

    This is the operation that broke as `math.floor(price / tick)`: `0.29/0.01` is `28.999999999999996`
    in float, so a bare floor gives 28 instead of 29. In exact arithmetic it is simply 29."""
    return ((value / step).to_integral_value(rounding=ROUND_FLOOR) * step).quantize(step)


def ceil_to(value: Decimal, step: Decimal = CENTICENT) -> Decimal:
    """Smallest multiple of `step` that is >= value. Exact — no pre-round guard needed.

    This is the operation that broke in the fee model: `0.07*0.5*0.5*1e4` is `175.00000000000003` in
    float, so a bare ceil charged a whole extra centicent. In exact arithmetic it is exactly 175."""
    return ((value / step).to_integral_value(rounding=ROUND_CEILING) * step).quantize(step)


def is_zero(value: Decimal, step: Decimal = CENTICENT) -> bool:
    """True if `value` is zero at the venue's resolution — the dust-safe replacement for `x > 0`.

    Book deltas leave ~1e-13 residue on removed levels; `qty > 0` treated that as real depth and
    selected a stale ghost as best-bid, producing phantom crossed books. Comparing at the venue's
    actual resolution makes that class of bug unrepresentable."""
    return abs(value) < step
