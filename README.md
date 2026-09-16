# Prediction-Market Maker

Public snapshot of a private working repo; commit history is squashed and market selection, tuning,
and results are withheld. An asynchronous Python research and market-making engine for regulated
prediction-market exchanges (**Polymarket US** and **Kalshi**). Built to model exact exchange
microstructures, enforce exact arithmetic, and execute safely under venue edge cases, rate limits,
and feed degradation. This tree also carries the safety rails a real-money process needs around the
engine, and the test harness that pins those rails.

**Claim legend.** Every claim in this file, `ARCHITECTURE.md` and `CASE_STUDY.md` carries one tag:

- **[IMPLEMENTED]** — the code is in this tree.
- **[TESTED]** — a test in this tree exercises it and goes red when the mechanism is removed.
- **[OBSERVED]** — a design lesson from operating the system, stated without numbers.

No claim here is about profitability, uptime or reliability. The instrument is published; the
readings are not (see `NOTICE.md`).

---

## Codebase at a glance

- **Line count:** `44,778` lines of Python across `99` files
- **Test suite:** `878` tests across `46` files, pure and mocked, no network (test functions
  found by AST at export; the pytest run collects more through parametrize — `EXPORT_REPORT.md`
  carries the run line)
- **Dependencies:** `7` third-party libraries, built directly on `asyncio` and `websockets`
- **Key modules:**
  - `bot/poly_us/maker.py` — Polymarket US quoting loop, inventory, caps, teardown
  - `bot/poly_us/client.py` — venue client; every request passes the shared budget
  - `bot/poly_us/feed.py`, `bot/poly_us/order_feed.py` — WS book feed and private order feed
  - `bot/kalshi/maker.py` — Kalshi maker, exact-`Decimal` L2 book maintenance
  - `bot/core/maker_state.py` — crash-durable, per-lane state and the recovery decision
  - `bot/core/venue_budget.py`, `bot/core/venue_backoff.py` — request budget and venue-health latch
  - `bot/core/teardown_cert.py`, `bot/core/run_manifest.py` — append-only evidence records
  - `bot/core/venue_close.py`, `bot/core/reconcile.py` — carried-position basis replay, position reconciliation
  - `bot/core/money.py` — exact `Decimal` grid arithmetic

---

## What is new since the last snapshot

Subsystem level; detail and support tags in [`CHANGES.md`](CHANGES.md).

- **Shared venue request budget** — one token bucket per source address, shared across every
  process on the host, with priority classes so a live maker's cancel is never queued behind a
  collector's crawl. **[IMPLEMENTED] [TESTED]**
- **Venue-health backoff latch** — a durable, advisory, fail-open latch that lets independent
  producers hold themselves back while the venue is refusing requests, instead of retrying on
  schedule and prolonging the outage. **[IMPLEMENTED] [TESTED]**
- **Teardown certification** — an append-only record of whether a teardown's post-cancel venue read
  actually verified flat, distinguishing "not certified" from "no record". **[IMPLEMENTED]**
- **Run manifest** — the resolved launch configuration written durably at start, so it outlives
  the process that ran it. **[IMPLEMENTED] [TESTED]**
- **Per-lane durable state** — one per-venue state file holding several concurrent lanes; each
  writer re-reads under an exclusive lock and replaces only its own lane. **[IMPLEMENTED]**
- **Alert delivery records** — delivery outcomes classified from the webhook's HTTP status and
  written to a local durable ledger, because a notifier cannot witness its own death.
  **[IMPLEMENTED] [TESTED]**
- **Post-exit order recovery owner** — a separate, bounded process that owns resting orders after
  a teardown whose sweep the venue could not confirm, and closes the crash record only on a fresh
  empty listing. The record it closes is in this tree; the owner process itself is withheld with
  the launch tooling. **[OBSERVED]**
- **Kalshi maker on exact `Decimal`** — the older engine's money fields are now parsed from the
  venue's string form; the last `float` money path in the tree is gone. **[IMPLEMENTED] [TESTED]**

---

## Architectural evolution

This engine grew out of [`arbitrage-engine`](https://github.com/spencerfletcher/arbitrage-engine),
a cross-venue taker system. That project's own measurements showed that the cross-venue
mispricings which persisted long enough to be caught were, by selection, the ones nobody faster
wanted — catchability and realness pull apart, and closing that gap needs colocation. **[OBSERVED]**

The pivot: stop racing to take, and get paid to rest on the book. A maker does not have to outrun
the taker; it has to keep correct quotes resting, account for fee rounding, and survive feed and
venue degradation without being picked off.

---

## Venue mechanics

- **Fee models as closed-form equations, not display tables.** Both venues publish a quadratic
  fee in price; the engine evaluates the published formula with the venue's own rounding rule
  (ceiling to the sub-cent on one venue, nearest-cent per order on the other). A display-table
  approximation disagrees with the formula at the rounding boundary, and the boundary is where a
  maker lives. **[IMPLEMENTED] [TESTED]**
- **The credit floor is a gate.** Because the maker credit is rounded per fill, a book whose price
  and size put the credit below the rounding boundary quotes perfectly, fills perfectly, and earns
  nothing. It is refused before quoting, not discovered in the accounting. **[IMPLEMENTED]**
  (the credit formula itself is **[TESTED]**)
- **CDN caching on public reads.** Public GET endpoints are cached in front of the venue; live-path
  reads bust the cache explicitly, and the WebSocket book is trusted only while its content
  freshness rails hold. **[IMPLEMENTED]**
- **Dynamic ticks.** Tick size varies by book. Because improving the touch costs a whole tick while
  the credit scales with price, tick resolution is mandatory before quoting and an unresolvable
  tick refuses the market. **[IMPLEMENTED]** (tick resolution in the client is **[TESTED]**)
- **Short-side price space.** One venue expresses an ask as a short buy priced in the same space as
  the long side, not at its complement; the client owns that translation so the maker never sees
  it. **[IMPLEMENTED] [TESTED]**

---

## Engineering for precision

**Floating-point elimination** (`bot/core/money.py`). Prices, quantities, fees and P&L are `Decimal`
parsed from the venue's string form — never from a float. Both historical precision bugs in this
project were float error amplified across a threshold: a fee ceiling landing on the wrong side of
a boundary, and residual level quantities that survived a `qty > 0` check and manufactured crossed
books. Exact grid quantization removes the class by construction. **[IMPLEMENTED] [TESTED]**
**[OBSERVED]**

**Venue behaviour pinned against captured responses.** Fixtures are the venue's real wire shapes,
not a hand-written belief; several of the sharpest bugs were places where documented and actual
behaviour disagreed (a post-only order documented as "rejected" that actually rests; a
fill-or-kill that partially filled). **[TESTED] [OBSERVED]**

---

## Crash resilience and process safety

The design assumption is SIGKILL: no `finally`, no `atexit`, no handler.

1. **Intent before transmission.** An order intent is fsync'd before the order is sent; the
   uncoverable window is "sent, then died before the response". **[IMPLEMENTED]**
2. **Durable writes.** Temp file → fsync → atomic replace → fsync directory. **[IMPLEMENTED] [TESTED]**
3. **Ordered teardown.** Cancel all → reconcile pending → passive flatten → venue sweep, driven off
   one ordered tuple; the kill switch halts into this sequence, never into a bare exit.
   **[IMPLEMENTED]**
4. **Feed degradation ladder.** Healthy WS → stale-book REST re-read → dark feed (reduced set,
   inventory-only quoting) → probation cleared only by a clean cycle. **[IMPLEMENTED]**
5. **Recovery is a different process** over a pure function of (durable state, venue orders, venue
   positions). An unreadable venue is cannot-verify and refuses; only a confirmed empty listing
   may close the record. The pure function (`assess_recovery`) is **[IMPLEMENTED] [TESTED]**; the
   process that calls it is withheld with the launch tooling.

---

## Running the tests

```
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest -q
```

The suite is pure and mocked: no network, no credentials, nothing under a real log directory.
`pytest.ini` runs the workers `pytest-xdist` provides; the conftest sandboxes every operational
rail (state files, notification delivery, the kill-switch file) with autouse fixtures.

---

## Limitations of the public suite

- **Withheld entirely:** the Polymarket US launch shim (the process that constructs, arms and runs
  the maker), the post-exit reconcile/recovery owner, the config printer, the market-selection
  pipeline and its registry values, the deployment and backup tooling, and every measurement.
  Where `ARCHITECTURE.md`, `CASE_STUDY.md` or `CHANGES.md` describe one of these, the tag says
  **[OBSERVED]**, not **[IMPLEMENTED]**.
- **Exported without their tests:** the Polymarket US maker (`bot/poly_us/maker.py`: quoting cycle,
  caps, teardown, feed ladder — its private suite imports the launch tooling and is withheld),
  `bot/core/venue_close.py`, `bot/core/teardown_cert.py`, `bot/core/fill_replay.py`,
  `bot/core/poly_activities.py`, the registry loader (`bot/core/probe_config.py`), and the
  `holds`, `tape_cache`, `tape_paths`, `archive_open`, `net_positions`, `maintenance` and
  `feed_health` helpers in `bot/core/`. The public suite imports these modules; it does not
  exercise their behaviour, and a claim about them carries **[IMPLEMENTED]** without **[TESTED]**.
- **What the public suite does run:** money arithmetic, durable writes, per-lane maker state and
  the recovery decision, position reconciliation, the run manifest, the venue budget's bucket,
  priority and ban rules, the venue-health backoff latch, alert delivery records, heartbeat, the
  memory guard, the Polymarket US venue client, raw WS handshake, private order feed and side
  semantics, the operator cancel tool, the Kalshi maker's guards, P&L and quoting arithmetic, the
  Kalshi book, fees, feeds, orders and queue tracker.
- The counts at the top of this file are computed at export time from this tree. No number here
  describes the private suite, and a green run here validates only what is listed above.
- Production-derived constants and defaults are WITHHELD: each carries a placeholder value chosen
  so the affected operation refuses, halts, or runs slower than any production setting, never a
  usable default, and the comment `placeholder — production value withheld`; `EXPORT_REPORT.md`
  lists them by name.
- **What cannot run without the withheld settings:** a `PolyMaker` constructed with the shipped
  defaults halts into teardown on the first cent of realized loss, refuses every park seat and
  every armed order TTL, halts at once when the feed goes dark, and paces requests below any
  production rate; the registry loader refuses any registry asking for more than one cell per
  second; the venue budget's ladder holds longer and admits less than production. These are
  refusals by design, not tuning suggestions. The Kalshi shim can run only with credentials, a
  production environment and its real-money flag, and its quoting constants are its own.
- The Polymarket US launch tooling is withheld. The included Kalshi launcher
  (`scripts/kalshi_live_mm.py`) can place real orders when explicitly configured and armed:
  credentials, a production environment, a configured series and its real-money flag.

---

## Repository layout

```
bot/
  poly_us/    Polymarket US maker (quoting, inventory, caps, teardown), WS book feed,
              private order feed, venue position reads, side semantics, venue client
  kalshi/     Kalshi maker, exact-Decimal L2 book, queue-attribution tracker
  core/       money, durable writes, per-lane crash state, venue budget and backoff,
              teardown certification, run manifest, carried-position basis replay,
              reconciliation, alert delivery records, feed health
scripts/      the Kalshi real-money arming shim, the operator cancel tool
tests/        pure, mocked, no network
config/       probe.example.json — the registry SHAPE, placeholder values only

ARCHITECTURE.md   data → decision → execution → settlement, the rails, the invariants
CASE_STUDY.md     why each rail exists, told as failure classes
CHANGES.md        what changed since the last snapshot
NOTICE.md         what is public, what is withheld, and why
```

---

## Scope

Withheld: runtime configuration (seat selection, sizing, cadence and threshold values), the
market-selection pipeline that produces it, measurement results, and deployment tooling. Production-derived constants and defaults are withheld (`README.md` § Limitations).
The Polymarket US launch tooling is withheld. The included Kalshi launcher can place real orders when explicitly configured and armed.

This is a personal research engine built for small-scale testing behind explicit arming gates, and
presented as a public technical artifact. `NOTICE.md` states exactly where the line falls.
