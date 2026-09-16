"""Tests for logger setup — guard against accidental Discord alerts in tests."""
import logging

from bot.core.logger import get_logger, _DiscordWebhookHandler


def test_no_discord_handler_under_pytest():
    """get_logger must not attach the Discord webhook handler while pytest runs,
    so error/warning logs during tests never fire real alerts."""
    log = get_logger("test_logger_no_discord")
    assert not any(isinstance(h, _DiscordWebhookHandler) for h in log.handlers)
