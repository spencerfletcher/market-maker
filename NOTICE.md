# Notice — public snapshot vs. private operation

This repository is a **public engineering snapshot** of a personal research project. It is complete
enough to read, test, and evaluate as a system, but it is intentionally **not a turnkey trading
deployment**. The separation below is deliberate, not an oversight.

## What lives here (public)

- The full maker architecture: the quoting cycle and every gate in it, inventory and cap handling,
  the WS book feed with its degradation ladder, the private order feed and its echo watchdog,
  belief recovery from the venue's trade ledger, teardown ordering, and crash recovery.
- The exact-`Decimal` money layer and the durable-write primitive, each function documenting the
  concrete failure it prevents.
- The reasoning behind every gate, threshold, and safety invariant — including the ones that turned
  out to be wrong the first time, and why.
- The test suite, including the mutation-tested safety fixes and the venue-behaviour pins.

## What is kept private (not in this tree)

- **Credentials and deployment config** — venue API keys, signing keys, and the production `.env`.
  Nothing here authenticates to any account.
- **Tuned production values** — position sizing, inventory and loss caps, requote intervals, and
  calibrated thresholds are supplied at runtime. The numeric defaults in the code are illustrative
  and, where they encode calibration, deliberately conservative.
- **Market selection — the answers, not the method.** The two-stage screener ships
  (`scripts/poly_market_screen.py`): its crawl, pacing, lane stratification and panel selection are
  public, because the *design* — cheap structural gates before any paid measurement — is the part
  worth reading. What does NOT ship is every input that makes it a live selection tool: the slug
  families it recognises, the forced-measurement watchlist and its per-family verdicts (the
  negative results especially), the scoring weights as tuned, and the discovery and slate-advisory
  tooling downstream of it, along with the launcher that consumes their output. Where a private
  list was removed, an illustrative placeholder shows the shape and is labelled as one; the code is
  runnable and honest, but it will not tell you which books to quote.
- **Measurement results.** Fill rates, queue-position outcomes, realized per-fill economics, capture
  rates, and every A/B verdict live in operational logs and private notes, not here. The code
  publishes the *instrument*; the readings stay private.
- **Deployment and operational tooling** — unit files, launch wrappers, supervisors, and the
  operational-observer stack.

## Why

Market-making at retail scale is capacity- and selection-constrained. The engineering generalizes;
the *edge* is almost entirely in knowing which books are worth resting in, at what price and what
size, and that knowledge degrades as more participants act on it. Publishing the engineering and the
measurement discipline is the intent. Publishing a runnable seat is not.

The practical test applied to every file: does it demonstrate *how the system reasons*, or does it
let someone skip the measurement and point capital at a book? The first ships; the second does not.

## A note on completeness

Because market selection and the operational stack are excluded, this tree will not run a live
session end to end as published. That is a consequence of the boundary, not an accident of
packaging. Everything that remains is real, tested, and imports cleanly, and the test suite passes
standalone.
