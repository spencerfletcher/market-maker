"""CLI entry point for the live paper market-maker. The implementation is `bot/kalshi/maker.py`.

This file exists to do ONE thing that cannot be done anywhere else: decide whether real orders are
enabled, and set `DRY_RUN` in the environment BEFORE `bot.core.config` is imported. `config`
snapshots the environment at import time, so the decision cannot move into `main()` — and it must
not live in the library, because a module-level `sys.argv` sniff under `bot/` fires on any process
that imports the module with that string in its argv, flipping DRY_RUN off as a side effect of an
import. Reading argv is a CLI's job.

⚠️ WHAT ACTUALLY GATES REAL ORDERS IS `config.DRY_RUN`, AND ONLY THAT. `MakerSession.quote` calls
`create_order` unconditionally; the sole short-circuit is `if config.DRY_RUN` inside
`KalshiClient.create_order`. `maker.main()`'s `real` flag gates INSTRUMENTATION — fills, markout,
preflight, the teardown stray sweep, the `run_id` mode stamp — not money.

An earlier version of this docstring claimed a two-condition guarantee. It did not exist, and the
gap it left was the dangerous one: a `.env` carrying `DRY_RUN=false` with no flag here would place
REAL quotes while printing "DRY PREVIEW", stamping rows `dry-`, logging no fills and skipping the
stray sweep. `maker.main()` now REFUSES TO START on that mismatch rather than running silently live.
So the flag's job is exactly one thing — set the env below so `config` comes up non-DRY — and any
disagreement between the two is a hard stop, not a downgrade.

Usage is `python -m scripts.kalshi_live_mm ...`; the implementation lives in
`bot/kalshi/maker.py` and this file is only the arming shim.
"""
from __future__ import annotations

import asyncio
import os
import sys

# MUST precede the maker import (which imports bot.core.config). Nothing else in this file may
# import anything from `bot` above this line.
if "--i-understand-real-money" in sys.argv:
    os.environ["DRY_RUN"] = "false"
# NB: KALSHI_ENV is deliberately NOT set — it comes from the deployed .env (prod), and
# `maker._live_guard` refuses to run against anything that is not prod.

from bot.kalshi import maker  # noqa: E402 — ordering is the point; see the module docstring

main = maker.main

if __name__ == "__main__":
    # A plain `kill` (SIGTERM) must run the teardown and unwind, not strand resting orders.
    # Installed here at the process entry point, before the loop starts, on the main thread.
    maker._install_sigterm_handler()
    asyncio.run(main())
