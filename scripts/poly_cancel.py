"""
scripts/poly_cancel.py
──────────────────────
List and cancel resting Polymarket US orders.

    .venv/bin/python -m scripts.poly_cancel                       # LIST (default, read-only)
    .venv/bin/python -m scripts.poly_cancel --slug <slug>         # list, scoped
    DRY_RUN=false .venv/bin/python -m scripts.poly_cancel --cancel <ID> --execute
    DRY_RUN=false .venv/bin/python -m scripts.poly_cancel --all --execute

⛔ WHY THIS EXISTS. On 2026-08-09 the maker refused to start with
`RECOVERY: RECOVER (live_venue_state) — cancel (always safe): BRKM6TS8YB9E`, i.e. its own refusal
told the operator to cancel a resting order — and **the repo had no tool that could cancel one**.
`scripts.poly_us_orders` is read-only (and ignores `--help`, printing the trade tape regardless).
The only way through was a one-off script written against the money path at 06:09 UTC. A maker
whose documented recovery step has no implementation is a maker that cannot be recovered at 3am.

⛔ CANCEL IS THE SAFE DIRECTION and this tool is deliberately EASY to run: removing a resting
order can only reduce exposure, never create it. That is why there is no `--i-understand-real-money`
gate here, unlike `poly_close` (which MOVES money and can open a mirror position). An emergency
control that is hard to fire is a worse failure than one fired unnecessarily.

⚠️ BUT IT MUST NEVER CLAIM A CANCEL IT DID NOT MAKE. `PolyUSClient.cancel_order_ex` returns
`(True, "ok")` under DRY_RUN **without contacting the venue** — so a dry-run "success" would read
identically to a real one and an operator would walk away from live orders. `--execute` therefore
REFUSES under DRY_RUN rather than reporting a no-op as done.
"""
from __future__ import annotations

import argparse
import asyncio
import sys

from bot.poly_us.client import PolyUSClient
from bot.poly_us.maker import venue_order_id


def _slug_of(order: dict) -> str:
    """⚠️ Both spellings appear in venue payloads; `maker.py` reads them the same way."""
    return str(order.get("marketSlug") or order.get("market_slug") or "")


def _describe(order: dict) -> str:
    oid = venue_order_id(order) or "<no id>"
    side = order.get("side") or order.get("orderSide") or "?"
    return (f"  {oid:<16} {_slug_of(order):<46} {str(side):<12} "
            f"px={order.get('price', '?')} size={order.get('size', '?')}")


async def run(args: argparse.Namespace) -> int:
    # `allow_during_ban`: a rate limit must never make an open position unmanageable —
    # this tool moves or verifies real inventory by hand (bot/core/venue_budget.py).
    client = PolyUSClient(allow_during_ban=True)

    # ⛔ RAISES on an unrecognised shape rather than returning [] — "no open orders" and "we could
    # not tell" must never be the same answer. Let it propagate: a cancel tool that reports
    # "nothing resting" because a read failed is the exact failure it exists to prevent.
    orders = await client.get_open_orders([args.slug] if args.slug else None)

    if not orders:
        print("no resting orders" + (f" on {args.slug}" if args.slug else ""))
        return 0

    print(f"{len(orders)} resting order(s):")
    for o in orders:
        print(_describe(o))

    targets: list[dict]
    if args.all:
        targets = orders
    elif args.cancel:
        targets = [o for o in orders if venue_order_id(o) == args.cancel]
        if not targets:
            print(f"\nREFUSING: {args.cancel} is not resting — it may have filled, been cancelled, "
                  f"or belong to another slug. Nothing sent.", file=sys.stderr)
            return 1
    else:
        print("\n(list only — pass --cancel <ID> or --all, plus --execute)")
        return 0

    if not args.execute:
        print(f"\nwould cancel {len(targets)} order(s) — pass --execute to send")
        return 0

    # ⛔ A DRY-RUN CANCEL IS A NO-OP THAT REPORTS SUCCESS. Refuse instead: the operator reading
    # "cancelled" here would walk away from live orders.
    if getattr(client, "_dry_run", False):
        print("REFUSING: client is in DRY_RUN — cancel_order_ex returns ok WITHOUT contacting the "
              "venue, so a 'success' here would be a lie. Run with DRY_RUN=false.", file=sys.stderr)
        return 2

    failures = 0
    for o in targets:
        oid, slug = venue_order_id(o), _slug_of(o)
        if not oid:
            print(f"  SKIP (no id): {slug} — cannot cancel what we cannot name", file=sys.stderr)
            failures += 1
            continue
        ok, verdict = await client.cancel_order_ex(oid, slug)
        # `not_found` means it is not resting — for a CANCEL that is the desired end state, not a
        # failure. The typed verdict exists precisely so this is distinguishable from a 502.
        if ok or verdict == "not_found":
            print(f"  cancelled {oid} ({slug})" + ("" if ok else "  [already gone]"))
        else:
            print(f"  FAILED {oid} ({slug}): {verdict}", file=sys.stderr)
            failures += 1

    if failures:
        print(f"\n⚠️ {failures} cancel(s) did not confirm — RE-READ before assuming flat.",
              file=sys.stderr)
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="List and cancel resting Polymarket US orders.")
    ap.add_argument("--slug", help="scope the listing to one market (server-side)")
    ap.add_argument("--cancel", metavar="ORDER_ID", help="cancel one order by venue id")
    ap.add_argument("--all", action="store_true", help="cancel every listed order")
    ap.add_argument("--execute", action="store_true",
                    help="actually send the cancels (default: list/report only)")
    args = ap.parse_args()
    if args.cancel and args.all:
        raise SystemExit("REFUSING: --cancel and --all together is ambiguous")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
