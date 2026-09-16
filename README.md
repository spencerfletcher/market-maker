# Prediction-Market Maker

Public snapshot of a private working repo; commit history is squashed and market selection, tuning, and results are withheld. An asynchronous Python research and market-making engine for regulated prediction-market exchanges (**Polymarket US** and **Kalshi**). Built to model exact exchange microstructures, enforce exact arithmetic, and execute safely under venue edge cases, rate limits, and feed degradation.

---

## Codebase At A Glance

- **Line Count:** 29,168 lines of Python across 54 files
- **Test Suite:** 684 passing tests in 2.6s across 14 test files (26s CI wall time)
- **Dependencies:** 11 third-party libraries (built directly on `asyncio` and `websockets`)
- **Key Modules:**
  - `bot/poly_us/maker.py` (3,931 LOC) — Polymarket reference quoting loop & state machine
  - `bot/kalshi/maker.py` (3,033 LOC) — Kalshi first-gen engine & exact L2 book maintenance
  - `scripts/poly_market_screen.py` (1,292 LOC) — Two-stage market screener
  - `scripts/poly_live_mm.py` (1,036 LOC) — Live execution arming shim
  - `bot/core/maker_state.py` (991 LOC) — Crash state durability & reconciliation
  - `bot/poly_us/feed.py` (856 LOC) — WebSocket book feed & echo watchdog
  - `bot/core/money.py` (116 LOC) — Exact `Decimal` grid arithmetic

---

## Architectural Evolution

This engine grew out of lessons learned from [`arbitrage-engine`](https://github.com/spencerfletcher/arbitrage-engine), a cross-venue taker arbitrage system. 

Analysis on the taker engine revealed a structural truth: **~69% of detected cross-venue arbitrage opportunities were illusions caused by stale feed reads.** Real mispricings disappeared within the round-trip network window. Competing in a latency race to take liquidity required colocation that wasn't viable.

The architectural pivot was simple: **stop racing to take, and get paid to rest on the book.** A market maker doesn't need to outrun the taker; it needs to maintain active quotes on viable books, account for fee structures and rounding floors, and survive feed outages without getting picked off.

---

## Venue Mechanics & Microstructure

### Fee Structure & Credit Floors
- **Kalshi Fee Model:** Evaluates closed-form contract fee equations rather than static display tables.
  $$\text{Taker Fee} = 0.07 \cdot C \cdot p \cdot (1-p)$$
  Ceiled to the centicent ($0.0001$). Maker fees evaluate to exactly $\frac{1}{4}$ of taker ($0.0175 \cdot C \cdot p \cdot (1-p)$).
- **Polymarket Maker Credits:** Modeled as $0.0125 \cdot p \cdot (1-p)$, rounded per fill increment rather than per order. If small taker fills round the earned credit down to zero, quoting that book loses money; the engine auto-refuses books where prices put the credit below the rounding floor.

### Rate Limits & CDN Cache Busting
- **Rate Budget:** Strictly throttled to 20 req/s per key+IP pair.
- **Cache Management:** Public GET endpoints carry `max-age=30s` CDN caching. Order-path reads explicitly bust cache headers to avoid quoting against stale book state.
- **Dynamic Ticks:** Tick sizes vary by an order of magnitude across books. Because touch improvement costs a flat tick per contract while credits scale with $p \cdot (1-p)$, tick resolution is mandatory before quoting.

---

## Engineering For Precision

### Floating-Point Elimination (`bot/core/money.py`)
All prices, sizes, fees, and P&L are represented as exact `Decimal` types initialized from strings—never native Python floats.

- **The Dust Bug:** Standard floating-point level removal left residual quantities around $\sim 1\text{e-}13$. A simple `qty > 0` check retained these ghost levels, which subsequently won `max()` selections. This manufactured crossed books and phantom edges on **30 of 52 trade triggers**—which happened to be the fattest perceived edges. Enforcing exact grid quantization in `bot/core/money.py` eliminated float dust by construction.
- **Formula vs. Display Table Reconciliation:** Replacing display-table fee approximations with closed-form equations recovered **10% (327 of 3,234)** of previously rejected trading opportunities.

### Venue Quirks & Execution Edge Cases
- **Short Price Space Alignment:** Exchanges evaluate short-side execution requests in Yes-space. Transmitting price complements mirrored resting ask orders, preventing short-position flattens from filling across **60% (268 of 449)** of execution candidates.
- **Silent FOK Rewrites:** Handled undocumented exchange behavior where 300-share Fill-or-Kill (FOK) orders filled 255 shares due to silent internal venue conversion to Immediate-or-Cancel (IOC).

---

## Crash Resilience & Process Safety

Designed around the principle that **a dying process cannot report its own death**.

1. **Pre-Transaction Persistence:** Order intents are `fsync`'d to disk *before* API transmission.
2. **Durability Discipline:** Writes follow a 3-step sequence: write temp file $\rightarrow$ `fsync` file $\rightarrow$ atomic `os.replace` $\rightarrow$ `fsync` containing directory.
3. **Strict Teardown Sequence:** `Cancel All` $\rightarrow$ `Reconcile Pending` $\rightarrow$ `Passive Flatten` $\rightarrow$ `Sweep`.
4. **Feed Degradation Ladder:**
   - **Healthy:** Full WS quoting loop.
   - **Stale Book:** REST re-read for affected book.
   - **Dark Feed:** Drop unmonitored books, restrict REST quoting to live inventory only, and cancel all dropped quotes.
   - **Probation & Recovery:** Requires a full clean WS cycle to clear outage state.

---

## Testing & Verification

- **Pure & Mocked Suite:** 684 unit and mocked tests executing in 2.6s (no network calls, fully sandboxed).
- **Mutation Testing:** Safety fixes are systematically verified by reverting code edits and requiring test failures (RED). In initial validation, 6 of 8 safety fixes had passed green suites despite being unpinned; test coverage was rewritten to enforce explicit failures.
- **Captured Real Fixtures:** Venue responses are pinned against captured wire logs rather than synthetic assumptions.

---

## Repository Layout

```
bot/
  poly_us/    Polymarket US maker reference implementation (quoting, inventory, cap, teardown, feeds)
  kalshi/     Kalshi first-gen maker, exact-Decimal L2 book, and queue-attribution tracker
  core/       Exact money math, durable state, crash recovery, loss cap ratchets, feed health
scripts/      Real-money arming shims, operator cancel tools, and market screeners
tests/        Pure/mocked test suite (14 files, 684 tests)

ARCHITECTURE.md   Technical map — quote cycle as a gate graph, and the SIGKILL/recovery
                  sequence, both as rendered diagrams
NOTICE.md         Exactly what is public, what is private, and where the line falls
```

---

## Scope & Disclaimer

This is a personal research engine built for small-scale testing behind explicit arming gates. It is presented as a public technical artifact. Specific market parameterizations, trading histories, and P&L results are withheld — [`NOTICE.md`](NOTICE.md) states exactly where that line falls and why.

For the technical map — the quote cycle drawn as a gate graph, and the SIGKILL/recovery sequence — see [`ARCHITECTURE.md`](ARCHITECTURE.md).
