"""Pins for the cancel tool. Cancel is safe-direction, but 'I cancelled it' must be TRUE."""
from __future__ import annotations

import argparse
import asyncio

import pytest

from scripts import poly_cancel


class _Client:
    def __init__(self, orders, dry_run=False, verdicts=None):
        self._orders, self._dry_run = orders, dry_run
        self._verdicts = verdicts or {}
        self.cancelled: list[tuple[str, str]] = []

    async def get_open_orders(self, slugs=None):
        return self._orders

    async def cancel_order_ex(self, oid, slug):
        self.cancelled.append((oid, slug))
        return self._verdicts.get(oid, (True, "ok"))


def _order(oid="BG4G3YHJM9ST", slug="example-usse-midterms-2026-11-03-rep"):
    return {"id": oid, "marketSlug": slug, "side": "sell", "price": "0.5430", "size": "44"}


def _args(**kw):
    base = dict(slug=None, cancel=None, all=False, execute=False)
    base.update(kw)
    return argparse.Namespace(**base)


def _run(client, args, monkeypatch):
    monkeypatch.setattr(poly_cancel, "PolyUSClient", lambda **kw: client)
    return asyncio.run(poly_cancel.run(args))


def test_a_DRY_RUN_execute_REFUSES_rather_than_reporting_a_no_op_as_done(monkeypatch, capsys):
    """⛔ `cancel_order_ex` returns (True, "ok") under DRY_RUN WITHOUT contacting the venue. A
    dry-run 'cancelled' is byte-identical to a real one, so an operator would walk away from live
    orders. This is the whole reason the tool checks."""
    c = _Client([_order()], dry_run=True)
    assert _run(c, _args(all=True, execute=True), monkeypatch) == 2
    assert c.cancelled == [], "nothing may be sent under DRY_RUN"
    assert "REFUSING" in capsys.readouterr().err


def test_it_reads_the_id_field_not_orderId(monkeypatch):
    """⛔ THE FIELD IS `id`. Reading `orderId` returned None for every order once and silently
    disabled the entire maker while 88 tests passed. This tool reuses `venue_order_id` rather
    than naming a field, and this pin fails if that is ever swapped for a literal lookup."""
    c = _Client([{"id": "REAL1", "orderId": "WRONG1", "marketSlug": "s"}])
    _run(c, _args(all=True, execute=True), monkeypatch)
    assert c.cancelled == [("REAL1", "s")]


def test_not_found_counts_as_cancelled_but_a_502_does_not(monkeypatch, capsys):
    """`not_found` = the order is not resting, which for a CANCEL is the desired end state. A
    transport error is NOT — collapsing the two is how a tool reports flat over live orders."""
    c = _Client([_order("A"), _order("B")],
                verdicts={"A": (False, "not_found"), "B": (False, "server_error")})
    assert _run(c, _args(all=True, execute=True), monkeypatch) == 1
    err = capsys.readouterr().err
    assert "FAILED B" in err and "did not confirm" in err


def test_without_execute_it_sends_nothing(monkeypatch):
    c = _Client([_order()])
    assert _run(c, _args(all=True), monkeypatch) == 0
    assert c.cancelled == []


def test_an_unknown_order_id_refuses_instead_of_cancelling_something_else(monkeypatch):
    c = _Client([_order("A")])
    assert _run(c, _args(cancel="NOPE", execute=True), monkeypatch) == 1
    assert c.cancelled == []


def test_an_unreadable_listing_PROPAGATES_rather_than_reporting_nothing_resting(monkeypatch):
    """`get_open_orders` RAISES on an unrecognised shape by design. A cancel tool that swallowed
    that and printed 'no resting orders' would produce the exact failure it exists to prevent."""
    class _Broken(_Client):
        async def get_open_orders(self, slugs=None):
            raise RuntimeError("unrecognised shape")

    with pytest.raises(RuntimeError):
        _run(_Broken([]), _args(), monkeypatch)
