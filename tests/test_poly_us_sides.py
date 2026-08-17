"""Tests for Poly US side-token encoding (bot/poly_us/sides).

An MLB moneyline game is ONE binary slug: the long side is one team, the short
side the other. We encode the short side as "<slug>::short" so the detector, feed,
and client can route it. These tests pin the round-trip + that a bare (long) slug
is never treated as short.
"""
from bot.poly_us.sides import short_token, parse_token


def test_short_token_appends_suffix():
    assert short_token("aec-mlb-tor-bos-2026-06-18") == "aec-mlb-tor-bos-2026-06-18::short"


def test_parse_long_token():
    slug, is_short = parse_token("aec-mlb-tor-bos-2026-06-18")
    assert slug == "aec-mlb-tor-bos-2026-06-18"
    assert is_short is False


def test_parse_short_token():
    slug, is_short = parse_token("aec-mlb-tor-bos-2026-06-18::short")
    assert slug == "aec-mlb-tor-bos-2026-06-18"
    assert is_short is True


def test_round_trip():
    slug = "atc-fwc-mex-rsa-2026-06-11-mex"
    assert parse_token(short_token(slug)) == (slug, True)


def test_bare_soccer_slug_is_long():
    # Soccer slugs never carry the suffix → always long, never misrouted.
    slug, is_short = parse_token("atc-fwc-mex-rsa-2026-06-11-mex")
    assert is_short is False
    assert slug == "atc-fwc-mex-rsa-2026-06-11-mex"
