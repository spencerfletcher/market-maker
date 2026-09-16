# Export report

- source revision: d686631eb89d6bad6cf6a6bee901c6fab42800c4
- files exported: 96 allowlisted + 14 overlay
- redactions applied (comment/docstring tokens, by class): {'never-path-deploy-unit': 6, 'unit-names': 20, 'override-comments': 139, 'docs-cites': 92, 'region': 1, 'money': 170, 'ticket-ids': 109, 'run-ids': 20, 'review-tags': 117, 'override-strings': 51, 'override-constants': 48, 'never-path-probe-registry': 7, 'amendments': 2, 'review-tag-continuation': 1, 'slugs': 1}
- transformed files: 55; every transformed file's AST with docstrings blanked is identical to its source except the declared string and constant overrides (85 sites)
- constants withheld (48), by name: ADVERSE_BAN_MAX_S, ADVERSE_RT_PER_CONTRACT, ADVERSE_WIDTH_K, CELLS_PER_SECOND_CEILING, CLOCK_GUARD_FILE, DEFAULT_MARK_TRIP_PER_CONTRACT, DEFAULT_MAX_REQ_PER_S, GUARD_WITNESS_MARGIN_S, LAG_HORIZON_S, MAX_PAGES, OWN_OFFSLATE_CARRY_LIMIT_USD, PACE_S, PARK_MIN_TICKS_OFF, PolyMaker.__init__:adverse_cooldown_s, PolyMaker.__init__:cap_fills, PolyMaker.__init__:flatten_wait_s, PolyMaker.__init__:loss_cap, PolyMaker.__init__:requote_s, PolyMaker.__init__:stale_cancel_cycles, RECONCILE_EVERY_CYCLES, SEED_SIZE_MAX, STARTUP_TOUCH_PACE_S, TEARDOWN_VERIFY_PACE_S, TTL_MIN_S, VENUE_BAN_BASE_S, VENUE_BAN_FLOOR_S, VENUE_BAN_MAX_S, VENUE_BAN_RESET_S, VENUE_BREAKER_S, VENUE_BREAKER_TRIPS, VENUE_BUDGET_BURST, VENUE_BUDGET_REQ_PER_S, VENUE_COLLECTOR_BUCKET_REQ_PER_S, VENUE_COLLECTOR_IDLE_S, VENUE_COLLECTOR_LEASE_REQ_PER_S, VENUE_COLLECTOR_LEASE_S, VENUE_COLLECTOR_REQ_PER_S, VENUE_COLLECTOR_WAIT_MAX_S, VENUE_RAMP_BURST, VENUE_RAMP_REQ_PER_S, VENUE_RAMP_S, VERIFY_CONFIRM_READS, WS_BOOK_STALE_S, WS_FALLBACK_MAX_BOOKS, WS_NEARCAP_GRACE_S, WS_REVERIFY_S, WS_SEED_TIMEOUT_S
- import closure: clean
- secrets scan (builtin): clean
- tests: rc=0 — 942 passed, 4 skipped in 5.57s
