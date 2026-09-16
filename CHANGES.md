# Changes since the last public snapshot

Subsystem level. Tags: **[IMPLEMENTED]** code in this tree; **[TESTED]** a test in this tree
exercises it and goes red when the mechanism is removed. No dates, no run identifiers, no values.

## Added

- **Shared venue request budget** (`bot/core/venue_budget.py`) — one locked token-bucket file per
  source address, shared across processes; priority classes (maker > operator tools >
  collectors); a limiter response sets a ban flag every acquirer refuses on; collectors refuse to
  draw beside a live maker heartbeat; a corrupt file refuses until repaired. Wired inside the
  venue client so every REST request passes it. **[IMPLEMENTED]** (bucket, priority and ban rules
  **[TESTED]**)
- **Venue-health backoff latch** (`bot/core/venue_backoff.py`) — durable, advisory, opt-in on the
  read side, fail-open; severity ordering, monotone deadline, expiry-only release; the money path
  never consults it. **[IMPLEMENTED] [TESTED]**
- **Teardown certification** (`bot/core/teardown_cert.py`) — append-only row per teardown from the
  venue-reading verdict; absence is not certification; cannot-verify records *not certified*.
  **[IMPLEMENTED]**
- **Run manifest** (`bot/core/run_manifest.py`) — append-only row per run start carrying the
  resolved configuration; per-book specs as one JSON column; read by header name.
  **[IMPLEMENTED] [TESTED]**
- **Post-hoc reconcile and post-exit recovery** (withheld with the launch tooling) — a fresh
  venue read against durable belief that appends a certification row only on an exact match;
  closes an uncertified crash record with the ledger intact only on a second empty strict listing;
  a bounded recovery owner for resting orders after an unverified sweep that sends nothing under a
  ban, never places, and pages once at the bound. **[OBSERVED]**
- **Alert delivery records** (`bot/core/alert_health.py`; failure log in `bot/core/alerts.py`) —
  delivery classified from the webhook status, written to a local durable ledger, never through
  the channel being judged; the recorder cannot raise. **[IMPLEMENTED] [TESTED]**
- **Registry loader** (`bot/core/probe_config.py`) — runtime knobs resolved from one file, failing
  closed at the point of use; the file supplies argparse defaults, a flag overrides, the banner
  names the source; arming flags stay CLI-only. Ships with `config/probe.example.json`, the
  file's shape with placeholder values only. **[IMPLEMENTED]**
- **Flatten orders with their own venue-side deadline**, independent of the lane's quote TTL, so
  a teardown under a ban leaves nothing resting indefinitely without a request from us.
  **[IMPLEMENTED]**

## Changed

- **Per-lane durable state** (`bot/core/maker_state.py`) — one per-venue file holds several
  concurrent lanes; each store binds to one lane, re-reads under an exclusive lock and replaces
  only its own subtree; a malformed lanes document is refused, never read as fewer lanes; a loss
  ledger on a session axis and a lifetime account axis. **[IMPLEMENTED]**
- **Teardown cancel set** — the maker's own pass cancels resting and venue-contradicted parked
  orders by id, not already-acknowledged cancels; a ban verdict ends the phase without retry and
  leaves the record open with the reason. **[IMPLEMENTED]**
- **WS book source for the maker's cycle** — the maintained cache serves the per-cycle read under
  content-freshness rails with a periodic fresh REST re-verify as the hard bound; a new tape
  column records which source served each quote. **[IMPLEMENTED]**
- **Kalshi maker on exact `Decimal`** (`bot/kalshi/maker.py`) — money fields parsed from the venue's
  string form; the tick is a `Decimal` constant; the last float money path in the tree is gone.
  **[IMPLEMENTED] [TESTED]**
- **Belief recovery from the venue's trade ledger** — when the order store disowns an order, the
  maker walks the trade ledger to heal belief and marks recovered fills on the tape.
  **[IMPLEMENTED]**
- **Launch refuses over an open unresolved-exposure row** until it is accepted or declared.
  The launch shim is withheld; the row and its reader are in the tree. **[OBSERVED]**

## Removed

- The cross-venue arbitrage runner and its detector are no longer in the tree; the predecessor
  project remains its own repository. **[IMPLEMENTED]**

## Test harness

- Parallel workers via `pytest-xdist` (a hard dependency; see `pytest.ini`). **[IMPLEMENTED]**
- The public conftest sandboxes every operational-rail path and the working directory per test;
  the private suite's log-directory write guard is not exported. **[IMPLEMENTED]**
- Test count: `878` (test functions found by AST at export; the pytest run collects more
  through parametrize).
