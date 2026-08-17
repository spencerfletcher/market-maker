# Prediction-Market Maker

Two market-making engines for regulated prediction-market exchanges — **Polymarket US** and
**Kalshi** — built to find out what actually makes money, and to keep being right about it as the
venues change underneath.

The engineering problem is not "quote a two-sided market"; that part is easy. It is that **almost
every number you would use to decide whether a strategy works is wrong the first time you measure
it**, usually in the direction you were hoping for. A fee model read off a display table instead of
the formula above it. A markout that counts the spread you captured as price quality. A queue
metric that turns out to count the people behind you. Each of those produced a confident,
profitable-looking conclusion here, and each was false. So the engines are built around measuring
their own economics honestly, refusing to trade on a number that has not survived being attacked,
and getting cheaper to correct when one turns out wrong — because the correction is not the
exception, it is the loop.

> **Status: a personal research engine, not a production desk.**
>
> Both engines are real programs that have quoted real books, at deliberately small size, under an
> explicit arming gate. The Polymarket maker is the current reference implementation and the one
> under active development; the Kalshi maker is the first-generation engine, kept in the tree
> because its book maintenance and its queue-attribution instrumentation are the best parts of it —
> and because a repository that only shows you the version its author is proud of is a sales
> brochure. The methods are public and the answers are not: which books to quote, the tuned
> parameters, and every measured result stay private, so this repository will show you how the
> decisions get made without handing over the decisions. See [`NOTICE.md`](NOTICE.md) for the exact
> line and why it falls there.

---

## How this got built

The predecessor to this repo is [`arbitrage-engine`](https://github.com/spencerfletcher/arbitrage-engine) — a
cross-venue arbitrage engine that took liquidity: find the same real-world event priced on two
exchanges, buy both sides for less than the dollar one of them must pay, keep the difference. That
engine works, and the measurement apparatus around it is the reason this one exists. It established,
against its own author's hopes, that the catchable edges were the ones that weren't real: the edges
that survived long enough to be taken were the ones a stale feed had invented, and the genuinely
mispriced ones were gone inside the round-trip. Speed was the wall, and the wall was not
crossable without colocation.

The conclusion was structural, not a tuning failure: **if you cannot win the race to take, stop
racing and get paid to wait.** A maker does not need to be faster than the market — it needs to be
resting when the market arrives, to be quoting books that can actually pay it, and to survive being
wrong about what it is holding. That reframing is the reason this repository exists, and the
measurement apparatus carried over with it: the arb engine's real product was never the trades, it
was knowing which of its own numbers to believe.

## Why market-making here is hard

Four constraints, all derivable from public venue documentation, and each one shapes the code:

- **The maker credit is a formula with a rounding floor, not a spread.** The venue pays a per-fill
  credit proportional to `p·(1−p)` and rounds it to the nearest cent *per fill increment*. That
  produces a **size floor below which a fill earns nothing at all** — the order rests correctly,
  fills correctly, and is worth zero. Worse, the rounding is applied to the taker's nibble size, not
  to your order size, so a book worked by dust-sized takers can forfeit the credit on a large
  fraction of otherwise perfect orders. The engine refuses at startup to quote a book whose price
  puts the credit below that floor, rather than quoting it and finding out later.
- **The tick is per-market and must be read, never assumed.** Ticks vary by an order of magnitude
  across the venue. Since improving on the touch costs a flat tick per contract while the credit
  scales with `p·(1−p)`, the economics of improvement are a *ratio*, and the same policy is correct
  on one book and ruinous on another. `get_market_tick` failing to resolve is a refusal to quote,
  not a default.
- **Every public GET is CDN-cached.** A book read that looks current can be up to the cache lifetime
  stale — long enough to place a quote against a price that no longer exists. Every read on the
  quoting path is explicitly cache-busted; only bulk discovery reads are allowed to be cached.
- **The rate budget, not capital, bounds concurrency.** How many books one process can hold is set
  by requests per second against the venue's limit, divided by the requote interval. Capital does
  not bind at this scale; the request budget does, and the WS book feed exists primarily to buy
  headroom against it.

## The quoting cycle

Per market, every requote interval:

1. **`is_paused()` first**, before any venue call. A kill switch checked *after* the book read has
   already spent a request and already decided a quote.
2. **Book read** — WS if the feed is healthy and the book's content age is inside the freshness
   bound, a cache-busted REST re-read of that one book otherwise. If either side of the touch is
   unreadable, refuse this market for this cycle. Unreadable is not "wide".
3. **Compute quotes** — join the touch, or improve by exactly one tick where the spread is at least
   two ticks — and record *which*, because joined fills and improved fills are different
   populations and pooling them makes the resulting statistics mean nothing.
4. **Cancel-then-replace only where our own price moved.** This is the dominant variable in the
   whole system, not a micro-optimization: an unchanged quote keeps its queue position, and amending
   to reprice forfeits it. A loop that re-places every cycle sits permanently at the back of every
   queue.
5. **Poll fills**, book them idempotently against the venue's cumulative quantity, and read the
   commission back per fill rather than modelling it.
6. **Check the loss cap** — after the fill poll, so a breach halts inside the same cycle that
   discovered it.

The hold-vs-replace decision is a pure function, and it compares prices **as `Decimal`s**, so
`0.4400` and `0.44` are the same price. A string compare there would forfeit the queue every single
cycle, silently, and every fill statistic downstream would be measuring the bug.

## Money is `Decimal`, end to end

Prices, sizes, fees, and P&L are `Decimal`, parsed from the venue's **string** wire form — never
`float`, and never `Decimal(0.47)`, which just launders a float's error into an exact type.

`bot/core/money.py` is 115 lines and every function in it documents the concrete production bug it
prevents. Two of them were real:

- a fee computation where the exact value was representable but the float wasn't, so a bare `ceil()`
  crossed a rounding boundary and overcharged the fee — which silently *understated* every edge the
  predecessor engine had ever logged;
- a book-maintenance path where removing a level left a residual quantity around `1e-13`, a
  `qty > 0` check kept the ghost, and the stale ghost level won a `max()` — manufacturing crossed
  books and phantom edges out of arithmetic noise.

Both were originally patched with a hand-placed `round()` before the amplifying operation. That
works and it is the wrong fix, because it makes correctness depend on every future author
remembering the guard — which is exactly how both bugs got in. The helpers make the guard
structural: `floor_to`/`ceil_to` round at the grid by construction, `is_zero` compares against the
grid rather than against zero, and `D()` refuses a `float` at runtime. The Kalshi book is maintained
as `dict[Decimal, Decimal]` — prices are their own exact dict keys — so delta arithmetic lands on
exactly zero and **the dust bug is unrepresentable rather than rounded away.** That is the general
principle the repo is trying to follow: prefer the fix that removes the bug from the space of things
the code can express.

**Where the discipline is not yet complete, stated plainly:** the Polymarket maker holds `Decimal`
through the order boundary into the client, which quantizes and refuses non-finite values. The
Kalshi maker — the first-generation engine — is still `float`-based, with a hardcoded tick and
hand-rolled half-tick comparisons. It predates this rule and has not been migrated. It is in the
tree as history, and it is labelled as history rather than quietly presented as current practice.

## Designed for SIGKILL

**The dying process cannot report its own death.** SIGKILL runs no `finally`, no `atexit`, no signal
handler — so any design where cleanup writes the record is a design that writes nothing precisely
when it counts. Four consequences shape `bot/core/maker_state.py` and `bot/core/durable.py`:

- **State is durable before it is needed, not after.** An order intent is fsync'd to disk *before*
  the order is sent. The uncoverable window is "sent, then died before the response arrived";
  recording after the venue answers leaves that window with no trace at all.
- **The `clean_exit` flag is written pessimistically** — `False` at run start, `True` only at a real
  exit. A crash therefore leaves the correct value on disk *by doing nothing*, which is the only
  behaviour SIGKILL reliably permits.
- **Liveness is asserted continuously by the living process**, and the *absence* of assertions is
  what a separate observer notices. Nothing is asked to announce its own failure.
- **Recovery is a different program.** `assess_recovery` is a pure function over
  *(durable state, venue open orders, venue positions)* and a separate CLI runs it. The dying
  process does not participate in its own recovery.

The durable write is a three-step argument, and step three is the one people drop: write to a temp
file and **fsync the file**, `os.replace` it into position, then **fsync the containing directory** —
without which the rename itself is not durable across a power loss.

Two rules govern every recovery decision:

- **Cannot-verify is not flat.** An unreadable venue is `None`; an empty list is confirmed-flat.
  Collapsing those two is not a style question — in the predecessor engine, a response-shape
  mismatch made a reconciler return `[]` for a month, so it reported "confirmed flat" on every poll
  while positions were open. `None ≠ [] ≠ 0`, and the type system is made to say so.
- **Cancelling is safe, flattening is not.** A cancel only removes exposure, so even an
  unattributable resting order is cancelled automatically — including under a refusal-to-start,
  because a refusal must not leave live orders resting overnight. A flatten *moves money* at a price
  something has to choose, so a venue position with no matching durable record halts for an
  operator instead.

## The loss cap: a ratchet, not a counter

The cap is durable and it only ever tightens. Every completed round trip feeds a price-realized
number into the per-venue state file, and a breach halts inside the same cycle into the full
teardown. It survives process death by construction — a crash-restart loop cannot silently reset the
budget, which is what an in-process counter does.

It binds on **two axes at once**:

- **Session** — what *this* run has realized.
- **Lifetime** — the sum of per-lane loss-to-date across the whole account, each lane floored at
  zero before summing. Losses only, never netted: allowing a profitable lane to offset a losing one
  would let one lane's profit *buy* another lane's loss budget, which is exactly the rule the cap
  already refuses within a single lane. Concurrent runs share one venue ledger, each binding to its
  own lane subtree under a file lock, so two lanes cannot clobber each other's record — and the
  account-wide read means N concurrent runs cannot lose N times the cap.

Three honest limits, stated in the code and worth repeating here: the maker credit — the entire
edge — is deliberately **not** credited against the cap, so it can trip on a lifetime that was
credit-profitable; unrealized marks are invisible until they realize; and in-session profit does
extend the remaining budget, because the axis is net.

## The WS book feed, and its degradation ladder

The book feed exists to buy request-budget headroom, and a feed that fails *quietly* is worse than
no feed — it hands the quoting loop a stale price with full confidence. So the maker treats the feed
as an untrusted subsystem with an explicit ladder:

| state | what the maker does |
|---|---|
| **healthy** | quote the full slate from WS books, subject to a content-age bound per book |
| **book too old** | fresh cache-busted REST re-read of *that one book*, full slate continues |
| **dark** | drop to a **reduced set** — every book with live inventory first, since an unquoted held book cannot work its way out — served over REST, and **cancel the quotes on every dropped book** rather than leaving them resting against a price nobody is watching |
| **probation** | the reduced set came back all-WS-fresh, so *try* the full slate again — but a reduced-set-only cycle does not count as recovery; only a clean full-slate cycle clears the outage |
| **relapse** | probation failed → back to reduced, cancels re-issued. Bounded by the fallback window |
| **halt** | dark past the fallback window, or dark at all while near the contract cap — a run that cannot verify the book has no business holding near-maximum inventory |

The state machine is the honest part: it distinguishes *"no frames at all"* from the **frozen-resend
zombie** — frames arriving on a live socket, book never changing, venue reporting no error — because
the freshness clock advances only on a *real* top-of-book change, not on message arrival. The
zombie is the one that quietly feeds phantom prices into a live quoting loop while every connection
health check reads green.

The private order stream gets the same treatment for a different reason: it has no snapshot, no
heartbeat, and no sequence number, and its subscription dies silently while the socket keeps
answering pings. So liveness there is an **echo watchdog** — every real placement expects its own
order id back within a deadline, and an expired expectation tears the connection down. A feed that
has demonstrably delivered *since* the action logs an anomaly instead of reconnecting, because
killing a working feed would storm. The stream is an accelerator only: fills booked from it go
through the same cumulative-quantity-idempotent path the REST poll then verifies, so the worst a
dead feed can do is make the maker slower, never blinder.

## Belief recovery: the venue is the truth

The maker's picture of what it holds is a *local belief* assembled from order responses, and every
such belief is one dropped response away from being wrong. Two mechanisms keep it honest:

- The venue's **order store** purges and lags — an order can vanish from it while a fill against it
  is real. So when the store disowns an order the maker believes in, the maker walks the venue's
  **trade ledger**, which survives, and heals belief from it. Recovered fills land on the tape
  marked as such, with the venue's own timestamp, so no downstream analysis can mistake a recovered
  fill for a promptly-booked one.
- A zombie order whose cancel probes complete is **logged and left resting** by default, not
  retired. Retiring it is gated behind an explicit flag, because "the venue says this order does not
  exist" and "this order does not exist" are different statements, and treating the first as the
  second lets a purged-and-filled order go unbooked while the shutdown sweep certifies the account
  clean over it.

## What gates real money

Three independent layers, in the order they fire:

1. **`config.DRY_RUN`, and only that**, actually enables orders — the venue client snapshots it at
   construction and every order method short-circuits on it.
2. **One file can set it**: the CLI shim, from an `sys.argv` sniff that must run *before*
   `bot.core.config` is imported, because config snapshots the environment at import time. That
   ordering constraint is enforced by a comment, by module layout, and by a test — and the sniff
   deliberately does **not** live under `bot/`, because a module-level argv read in a library fires
   on any process that happens to import it with the right string in argv, flipping the safety off
   as a side effect of an import.
3. **`arming_refusal`** converts a mismatch into a **hard stop** rather than a silent downgrade to
   dry. The dangerous combination is not "flag with no config" — it is a `.env` already carrying
   `DRY_RUN=false` with *no* flag, which would place real quotes while the run reported a preview.
   The refusal is evaluated before any venue object is constructed.

Shadow mode is a fourth, structurally different barrier: the placement method **raises** rather than
returning, so "this run placed nothing" is a property of the code path, not of a branch that could
be one bad edit away from wrong.

## Kill switch and teardown ordering

`is_paused()` is the first statement of every cycle. When it trips, the maker halts *into the full
teardown* rather than simply exiting, and the teardown order is load-bearing:

**cancel-all → reconcile-pending → flatten → sweep last.**

Each ordering constraint exists because reversing it is wrong in a specific way:

- **Cancel first** — every cycle spent deciding is a cycle those orders are still live.
- **Reconcile before flatten** — venue order reads lag, and a flatten sized against a pre-verify
  belief can try to dispose of a position the account does not hold. The flatten must size itself
  *after* the delayed verify, and paying that latency is the point of the phase.
- **Sweep last** — sweeping before flattening re-reads a book the flatten is about to move, so it
  certifies a state that is already stale, and the flatten's own residual order escapes the sweep
  entirely.

The phases are driven off one ordered tuple with dispatch by name, so the invariant is a single
pinnable value rather than four statements that can drift apart — which is precisely what happened
to the first-generation engine, where the documented order and the implemented order disagreed in
four places.

The crash record closes **only on the venue's evidence**: the durable run record is cleared and the
run marked ended only when the open-orders listing actually succeeded, every order on this run's
markets cancelled cleanly (a refused cancel is *counted*, never swallowed), and nothing was
unattributable. Otherwise the record stays open and the **next start refuses** — a maker that cannot
prove it left the account flat does not get to open a new position on top of the uncertainty.

## What it deliberately does not do

- **It never market-sells to flatten.** Cancelling only removes exposure, so it is automated;
  flattening moves money at a price something must choose. A residual surviving the passive flatten
  window is *reported to an operator* and left in the durable record for the recovery tool.
- **It does not resolve its own refusals.** A refusal to start is a state a human clears, with the
  procedure printed in the refusal message.
- **It does not decide what to quote.** Book selection is a separate concern, held privately, and
  the launch path refuses a book it cannot freshness-check.

## Testing

Pure and mocked — no network, no credentials, runs in about twenty seconds.

```bash
python -m pytest -q
```

The suite is curated around eight disciplines rather than around line coverage: the credit model
including the rounding floor; `Decimal` exactness at the exact thresholds where the two real float
bugs lived; the loss-cap ratchet across processes on both axes; kill-switch teardown ordering, and
that a pause *cancels* before it halts; queue-position attribution *and its refusals to report*;
requote logic, including that equal `Decimal` prices yield HOLD; crash recovery as a pure function,
with `None ≠ {}`; and the WS degradation ladder end to end.

Two disciplines make it worth trusting. Safety fixes are **mutation-tested** — reverting the fix
must turn a specific test red, because a green suite that stays green with the fix removed has
pinned nothing (measured on one such batch: six of eight "tested" fixes were not actually pinned).
And venue behaviour is pinned against **captured real responses**, because a fixture written from
the same belief as the code can only ever confirm it.

The conftest is itself part of the argument: autouse fixtures sandbox every operational rail, so a
test run cannot write to a production tape, post a real notification, or read the operator's kill
switch — each one added after the corresponding incident.

## Repository layout

```
bot/
  poly_us/    Polymarket US maker — the reference implementation: quoting loop,
              inventory, cap, teardown, WS book + order feeds, side semantics
  kalshi/     first-generation maker, plus the exact-Decimal L2 book and the
              queue-attribution tracker
  core/       money (exact Decimal), durable writes, crash state + recovery,
              safety caps, feed health, config, logging, redaction
scripts/      the real-money arming shims, operator cancel tool, config printer,
              and the two-stage market screener (structural gates before paid reads)
tests/        pure/mocked suite
ARCHITECTURE.md   the technical map, with diagrams
NOTICE.md         what is public, what is private, and why
```

## Tech

**Python · asyncio · websockets · REST · pytest.** Concurrency and failure handling are hand-written
on `asyncio` — no framework hiding the control flow — so every reconnect, timeout, and race is
explicit and testable. The dependency list is eleven entries and each one earns its place.

## Scope & disclaimer

A personal research project. Nothing here is financial advice or an invitation to trade. Live
running has been at deliberately small size behind an explicit arming gate, and every measured
result — fill rates, realized economics, which books are worth quoting — is held privately.

This is a **public engineering snapshot**: complete enough to read, test, and evaluate as a system,
and intentionally not a turnkey deployment. Credentials, tuned production values, and market
selection are loaded at runtime from configuration that is not in this tree. See
[`NOTICE.md`](NOTICE.md).
