"""
bot/core/poly_activities.py
───────────────────────────
The Poly trade-activities parser, moved VERBATIM out of `scripts/capital_ledger.py` [2026-09-15]
so `bot/core/venue_close.py`'s hand-close walk imports it from inside `bot/`. `capital_ledger`
re-imports every name from here — ONE `Txn` class, ONE parser, the same three named traps:
`effectiveRealizedPnl` over `realizedPnl`, the `isAggressor` leg selection, the per-execution
commission.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from bot.core.money import dec_or_none

# ── parsing primitives: Decimal from the venue's STRING form ─────────────────────────────────

def _money(v) -> Decimal | None:
    """Poly wraps money as `{"value": "0.8510", "currency": "USD"}`. Take the string."""
    if isinstance(v, dict):
        return dec_or_none(v.get("value"))
    return dec_or_none(v)


def _fmt(v: Decimal | None) -> str:
    """CSV cell. `None` → blank. A real zero → "0". These must never collapse together."""
    return "" if v is None else str(v)


# Moved to bot/core/venue_time.py [belief-recovery v5 §5 / r4 N2] — the maker's recovery walk
# needs the same parse and bot/ must not import scripts/. Re-exported under the old name so
# existing importers (poly_activities_probe, the tests) are untouched.
from bot.core.venue_time import iso_to_ts as _iso_to_ts  # noqa: E402


def _ts_to_iso(ts: float | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(float(ts), timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Txn:
    ts: float | None
    venue: str
    txn_id: str
    kind: str                      # trade | settlement | fee | rebate | credit | unmapped:<type>
    market: str = ""
    side: str = ""
    qty: Decimal | None = None
    price: Decimal | None = None
    cash: Decimal | None = None
    fee: Decimal | None = None
    rebate: Decimal | None = None
    realized: Decimal | None = None
    source: str = ""
    note: str = ""

    def to_row(self) -> list[str]:
        return [_fmt(dec_or_none(self.ts)), _ts_to_iso(self.ts), self.venue, self.txn_id, self.kind,
                self.market, self.side, _fmt(self.qty), _fmt(self.price), _fmt(self.cash),
                _fmt(self.fee), _fmt(self.rebate), _fmt(self.realized), self.source, self.note]


# ── Poly: activities → transactions ──────────────────────────────────────────────────────────

def _our_execution(trade: dict) -> tuple[dict | None, str]:
    """TRAP 2. `isAggressor` selects our leg; the other one is the COUNTERPARTY's and its
    commission has the opposite sign. Returns (execution|None, note-fragment)."""
    is_agg = trade.get("isAggressor")
    if is_agg is None:
        return None, "no_own_execution(isAggressor absent — cannot tell which leg is ours)"
    ours = trade.get("aggressorExecution") if is_agg else trade.get("passiveExecution")
    if not isinstance(ours, dict):
        return None, (f"no_own_execution(isAggressor={is_agg} but that leg is absent — "
                      f"the other leg belongs to the counterparty)")
    return ours, "role=aggressor" if is_agg else "role=passive"


def _unmapped_poly_txn(activity: dict) -> Txn:
    """An activity type we do not score. Recorded anyway — "complete records" is the point, and
    a dropped row is invisible at filing time. Money columns stay blank; the id is a content
    hash so a re-run deduplicates it."""
    kind = str(activity.get("type") or "ACTIVITY_TYPE_UNKNOWN")
    blob = json.dumps(activity, sort_keys=True, default=str)
    digest = hashlib.sha1(blob.encode()).hexdigest()[:16]
    payload = activity.get(kind.replace("ACTIVITY_TYPE_", "").lower()) or {}
    ts = _iso_to_ts(payload.get("createTime") if isinstance(payload, dict) else None)
    return Txn(ts=ts, venue="poly", txn_id=f"poly:unmapped:{digest}",
               kind=f"unmapped:{kind.lower()}", source="poly:portfolio.activities",
               note="unmapped_activity_type — recorded for completeness, NOT scored")


def poly_transactions(activities) -> list[Txn]:
    """One Txn per Poly activity. See TRAPS 1–3 in the module docstring."""
    out: list[Txn] = []
    for a in activities or []:
        if not isinstance(a, dict):
            continue
        trade = a.get("trade")
        if not isinstance(trade, dict) or not trade.get("id"):
            out.append(_unmapped_poly_txn(a if isinstance(a, dict) else {}))
            continue

        ours, role_note = _our_execution(trade)
        notes = [role_note]

        # TRAP 3: the per-EXECUTION commission. `commissionNotionalTotalCollected` on the order
        # is that ORDER's running total and repeats across partials.
        fee = rebate = None
        if ours is not None:
            comm = _money(ours.get("commissionNotionalCollected"))
            if comm is None:
                notes.append("commission_unreadable")
            elif comm < 0:
                fee, rebate = Decimal(0), -comm       # negative = rebate paid to us
            else:
                fee, rebate = comm, Decimal(0)

        order = ours.get("order") if isinstance(ours, dict) else None
        side = ""
        if isinstance(order, dict):
            # venue-reference discipline: direction comes from side + outcomeSide together.
            # `intent` does NOT give the direction of a close — the dem close carried
            # ORDER_INTENT_SELL_SHORT while REDUCING a short [deep-scoring §, ⚠️ note].
            side = f"{order.get('side', '?')}/{order.get('outcomeSide', '?')}"
            notes.append(f"intent={order.get('intent', '?')}")
            notes.append(f"order_id={order.get('id', '?')}")

        # TRAP 1: effectiveRealizedPnl, never its sibling. Null on an opening/adding trade
        # (430 of the 433 recorded activities) — which is blank, not zero.
        realized = _money(trade.get("effectiveRealizedPnl"))

        out.append(Txn(
            ts=_iso_to_ts(trade.get("createTime")),
            venue="poly",
            txn_id=f"poly:trade:{trade['id']}",
            kind="trade",
            market=str(trade.get("marketSlug") or ""),
            side=side,
            qty=dec_or_none(trade.get("qtyDecimal") or trade.get("qty")),
            price=_money(trade.get("price")),
            cash=_money(trade.get("cost")),
            fee=fee,
            rebate=rebate,
            realized=realized,
            source="poly:portfolio.activities",
            note=" ".join(n for n in notes if n) + " cash_basis=venue_cost(net_of_commission)",
        ))
    return out
