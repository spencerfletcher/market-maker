# Notice — public snapshot vs. private operation

This repository is a **public engineering snapshot** of a personal research project. It is complete
enough to read, test and evaluate as a system, and it is intentionally **not a turnkey trading
deployment**. The separation below is deliberate.

## Licensing

The code is released under the MIT License in `LICENSE`. The license covers what is in this tree;
it grants nothing about, and this repository contains nothing of, the private configuration and
measurements described below.

## What lives here (public)

- The maker architecture: the quoting cycle and every gate in it, inventory and cap handling,
  the WS book feed and its degradation ladder, the private order feed and its echo watchdog,
  belief recovery from the venue's trade ledger, teardown ordering, teardown certification and the
  crash-recovery decision.
- The shared venue request budget and the venue-health backoff latch.
- The exact-`Decimal` money layer and the durable-write primitive, each function documenting the
  concrete failure it prevents.
- The registry loader that resolves runtime configuration, with an example of the file's shape
  and no real values.
- The public subset of the test suite: the venue-behaviour pins and the rails' tests. The maker's
  own tests import the launch tooling and stay private; `README.md` § Limitations lists exactly
  what runs here.

## What is withheld (not in this tree)

Withheld: runtime configuration (seat selection, sizing, cadence and threshold values), the
market-selection pipeline that produces it, measurement results, and deployment tooling. Production-derived constants and defaults are withheld (`README.md` § Limitations).
The Polymarket US launch tooling is withheld. The included Kalshi launcher can place real orders when explicitly configured and armed.

In particular:

- **Credentials and deployment config** — venue API keys, signing keys, the production
  environment file, unit files, the Polymarket US launch shim and the post-exit recovery owner,
  supervisors and the operational-observer stack. Nothing here authenticates to any account.
- **Tuned production values** — position sizing, inventory and loss caps, requote cadence, latch
  and freshness thresholds. Each production-derived constant ships as a placeholder marked
  `production value withheld`, chosen so the operation refuses or runs slower rather than with a
  usable default (`README.md` § Limitations names what cannot run); the registry example carries
  placeholder values only.
- **Market selection — the answers, not the method.** Which books are quoted, how they are ranked,
  which classes are admitted or refused, and the discovery and slate tooling that produces those
  lists.
- **Measurement results.** Fill outcomes, queue-position outcomes, realized economics, rebate and
  capture figures, per-market verdicts and every A/B result. The code publishes the instrument;
  the readings stay private.
- **Venue-relationship facts** — request budgets as deployed, source addresses, and rate-limit
  history.

## Why

Market-making at retail scale is capacity- and selection-constrained. The engineering generalizes;
the edge is almost entirely in knowing which books are worth resting in, at what price and what
size, and that knowledge degrades as more participants act on it. Publishing the engineering and
the measurement discipline is the intent. Publishing a runnable seat is not.

The test applied to every file: does it demonstrate *how the system reasons*, or does it let
someone skip the measurement and point capital at a book? The first ships; the second does not.

## A note on completeness

Because market selection and the operational stack are excluded, the Polymarket US maker cannot
be started from this tree; the Kalshi launcher can, when explicitly configured and armed. That is
a consequence of the boundary, not an accident of packaging. Everything that remains is the real
code, imports cleanly, and the public test suite passes standalone; `README.md` § Limitations
names the modules that ship without their tests.
