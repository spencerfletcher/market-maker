"""Pins `bot/core/venue_time.ts_to_iso`, the one epoch→stamp renderer (was 8 private `_iso`
copies across scripts/). The stamp lands in operator tables and CSV cells, so the format and the
truncation rule are both load-bearing."""
from bot.core.venue_time import ts_to_iso


def test_ts_to_iso_renders_utc_with_the_year_and_the_Z():
    assert ts_to_iso(1787772088) == "2026-08-26T19:21:28Z"


def test_ts_to_iso_truncates_sub_second_never_rounds():
    assert ts_to_iso(1787772088.9) == ts_to_iso(1787772088)
