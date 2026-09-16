# Case study — why the rails exist

An engineering narrative, told as failure classes. Nothing here is a number; every number this
project produced is a measurement, and measurements stay private (`NOTICE.md`). Claim tags:

- **[IMPLEMENTED]** — the code is in this tree.
- **[TESTED]** — a test in this tree exercises it and goes red when the mechanism is removed.
- **[OBSERVED]** — a design lesson from operating the system, stated without numbers.

## Where this came from

The predecessor, a cross-venue taker, delivered its verdict through its own instrument: the
mispricings that persisted long enough to catch were, by selection, the ones nobody faster wanted.
Catchability and realness anti-correlate, and you never get settlement labels on the edges you
were too slow to fill — so the region where the real ones might live is structurally invisible.
The honest response was to believe the instrument, retire the strategy without deploying capital,
and ask the inverted question: if you cannot win the race to take liquidity, what does it pay to
be the liquidity? **[OBSERVED]**

The maker inherited the venue integrations, the durable-state discipline and the measurement
habits. It also inherited the reading rule that shaped everything after: a marginal rate is never
the verdict; the conditional on the selected sample is. A high fill rate is not good news on its
own — filling selects for the orders the market wanted to hit. **[OBSERVED]**

## Failure class 1 — a book that is fresh by the clock and stale in content

The WebSocket book feed's characteristic failure is not silence. It is a socket that answers
pings and keeps delivering frames whose content never changes. A liveness check keyed on message
receipt sees a healthy feed; the maker prices off a frozen touch. **[OBSERVED]**

The rail: freshness is a content signal. The per-book clock advances only when the venue's
transaction time changes; a slate-wide predicate forces reconnect when nothing has moved while
books should be moving; and a periodic fresh REST read is the hard bound on how long the cache is
trusted at all. A disagreement between the WS arm and the REST arm is interpreted according to
which arm served the quote, and it reaches the cycle tape so the read is accountable afterwards.
**[IMPLEMENTED]**

The lesson underneath it: a "fresh timestamp, stale content" gate rebuilt on receipt time is not
a fix, it is the same blind spot with a new name. The signal has to be the content.

## Failure class 2 — the venue bans the address during the teardown

A teardown is a burst by nature: cancel everything, read positions, place passive closes, sweep.
It is also the moment a limiter in front of the venue is most likely to refuse, because it counts
the whole address and the maker is not the only process on it. Independent collectors on their
own timers, each retrying an erroring route on schedule, produce exactly the burst shape a limiter
punishes — and every request during the refusal extends it, so blind retries prolong the outage
they are reacting to. **[OBSERVED]**

Three rails, layered:

- **A shared request budget** that lives outside the processes, since per-process pacing cannot
  see the sum. Priority classes put a live maker's cancel ahead of any collector, and a limiter
  signal sets a flag that every acquirer refuses on, because the correct response to a ban is
  silence. **[IMPLEMENTED] [TESTED]**
- **A venue-health latch** that lets producers hold themselves back — not stop, since a stopped
  producer silently stays stopped, and not retry, since that is the ban. Fail-open, loudly: a
  false latch costs one pass, while a latch that cannot lapse would block launches.
  **[IMPLEMENTED] [TESTED]**
- **Explicit ownership after exit.** The maker's own teardown pass is one pass with no loop: a ban
  verdict on a cancel ends the phase, the flatten orders carry their own venue-side deadline, and
  the record is left open with the reason that the sweep went unverified. A separate, bounded
  process then owns the resting orders: silent while a ban stands, one strict listing, cancels by
  id, a second strict listing, and closure only on an empty result. A refusal that never left the
  host is not an attempt; venue-answered attempts are bounded, and at the bound the lane pages once
  and stops. The maker's pass is **[IMPLEMENTED]**; the owner process is withheld **[OBSERVED]**.

The lesson: retries inside the dying process are the wrong owner. The question "who owns these
orders now?" needs an answer that survives the process, respects the venue's refusals, and ends.

## Failure class 3 — a fill the process saw and the ledger did not

A process designed around `finally` writes nothing precisely when it matters, because the death
that actually happens is SIGKILL — from an out-of-memory killer, from a supervisor, from a host.
An in-process loss counter resets on restart; a record written at exit is never written.
**[OBSERVED]**

The rails: state is durable before it is needed. The order intent is fsync'd before transmission;
the clean-exit flag is written pessimistically at start so a crash leaves the correct value by
doing nothing; the loss ledger is durable on a session axis and a lifetime axis; and the realized
writer fires per reducing fill, not per completed round trip, because a round trip that never
completes is exactly the fill that goes unrecorded. Recovery is a different process over a pure
function of durable state and two venue reads, with cannot-verify distinct from flat.
**[IMPLEMENTED]** (durable writes and the recovery decision are **[TESTED]**; the ledger axes are not)

Two later additions closed gaps the first version left open. A **teardown certification** row
records whether the post-teardown venue read actually verified flat, so the verdict no longer has
to be remembered by a human overnight; absence is not certification, and an unreadable venue
records *not certified* rather than nothing. A **run manifest** records the resolved launch
configuration durably, because a configuration that lives only in a process's command line dies
with it and has to be re-derived later from things that should not be trusted.
**[IMPLEMENTED]** (the manifest is **[TESTED]**; the certification writer is not)

The state file itself grew a rule: several lanes share one per-venue file, each writer re-reads
under an exclusive lock and replaces only its own subtree, and a document that does not parse as
well-formed lanes is treated as a mid-write artefact, never as "fewer lanes" — because reading it
as fewer lanes would silently delete a lane's carried loss and its crash record.
**[IMPLEMENTED]**

## Failure class 4 — an alert channel that cannot witness its own death

Every guard that "alerts the operator" was reporting into a void, and nothing could say so. The
webhook client returned a status for a revoked endpoint rather than raising; the dispatcher logged
the outcome at a level nobody reads; and the only place the failure could have been reported was
the channel that had failed. **[OBSERVED]**

The rail: delivery is classified from the HTTP response and written to a local durable ledger,
deliberately not to the notifier. The recorder never raises, because it runs inside the execution
lock during the fire window, and the bookkeeping about a best-effort alert must be less capable of
failing than the alert. A failed page from the recovery owner lands in a local failure log with
its body. **[IMPLEMENTED] [TESTED]**

The lesson generalizes: a subsystem must not be the only witness to its own state. The same
fallacy — the tracker as the only witness to positions — is why every position read goes to the
venue.

## Failure class 5 — float error amplified across a threshold

Both precision bugs in the project's history had the same shape: a float carried a tiny error, and
a ceiling, a floor, or a compare-to-zero amplified it into a wrong decision. A fee that ceilinged
to the wrong sub-cent. Residual level quantities that passed `qty > 0`, won a `max()`, and
manufactured crossed books — the fattest-looking edges were the phantoms. **[OBSERVED]**

The fix was structural rather than a guard. Both venues speak decimal strings on the wire, so the
lossy intermediate was ours; parse to `Decimal` from the string and quantize explicitly at every
rounding site with the venue's own rule. The migration was phased strangler-fig style, each phase
green, because `Decimal + float` raises at the seam and a half-migrated state fails loudly rather
than corrupting silently. The older Kalshi engine was the last float money path in the tree and is
now exact. **[IMPLEMENTED] [TESTED]**

## The test discipline that holds it together

A safety fix is verified by reverting it and requiring a specific test to go red. On one audited
batch, most of the "fixed" bugs had green suites that stayed green with the fix removed — they had
pinned nothing. Since then a fix without a red mutation is not a fix, and the ritual has caught its
own bad fixtures. **[TESTED] [OBSERVED]**

Fixtures are the venue's captured wire shapes, because a fixture written from the code's belief can
only confirm it, and the sharpest bugs were places where the venue's documented and actual
behaviour disagreed. Expected values derive from the constant they exercise, so a tuning change
never forces a test change, and the tests never assert a shipped value. **[TESTED]**

## What this is, honestly

A measurement discipline (separate channels, venue strings as the source of truth, refusal on an
unreadable read), an operator process built to surface its own optimistic errors, and a set of
rails each of which exists because a specific thing went wrong. Whether the measurements were
good enough to act on, and what the rails cost or saved, are results and stay private. The strategy, the sizing, the books and the results are the
private part; the reasoning is the public part.
