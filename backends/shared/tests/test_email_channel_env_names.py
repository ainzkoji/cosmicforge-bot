"""EmailChannel accepts both spellings of the SMTP password / sender variables.

Settings and .env.example use SMTP_PASSWORD / SMTP_FROM_EMAIL; the channel
originally read SMTP_PASS / SMTP_FROM only. Both work; the new names win.
No network: smtplib.SMTP is replaced by a fake.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from shared_lib.notifications.channels import email as email_channel

SMTP_VARS = ("SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "SMTP_PASSWORD", "SMTP_FROM", "SMTP_FROM_EMAIL")


@pytest.fixture
def smtp(monkeypatch):
    for name in SMTP_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SMTP_HOST", "smtp.example.test")
    monkeypatch.setenv("SMTP_USER", "mailer")
    server = MagicMock()
    factory = MagicMock()
    factory.return_value.__enter__.return_value = server
    monkeypatch.setattr(email_channel.smtplib, "SMTP", factory)
    return server


def _send() -> bool:
    return email_channel.EmailChannel.send("to@example.test", "subject", "<p>hi</p>", "hi")


@pytest.mark.parametrize("env,expected_password,expected_sender", [
    ({"SMTP_PASS": "old-pass", "SMTP_FROM": "old@example.test"}, "old-pass", "old@example.test"),
    ({"SMTP_PASSWORD": "new-pass", "SMTP_FROM_EMAIL": "new@example.test"}, "new-pass", "new@example.test"),
    ({"SMTP_PASS": "old-pass", "SMTP_PASSWORD": "new-pass",
      "SMTP_FROM": "old@example.test", "SMTP_FROM_EMAIL": "new@example.test"}, "new-pass", "new@example.test"),
    ({"SMTP_PASS": "old-pass", "SMTP_PASSWORD": "", "SMTP_FROM": "old@example.test", "SMTP_FROM_EMAIL": ""},
     "old-pass", "old@example.test"),
    ({"SMTP_PASSWORD": "new-pass"}, "new-pass", "noreply@cosmicforge.bot"),
])
def test_both_variable_names_are_accepted_and_new_names_win(smtp, monkeypatch, env, expected_password, expected_sender):
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    assert _send() is True

    smtp.login.assert_called_once_with("mailer", expected_password)
    sender, recipient, _message = smtp.sendmail.call_args.args
    assert (sender, recipient) == (expected_sender, "to@example.test")


def test_unconfigured_smtp_still_reports_failure(smtp, monkeypatch):
    monkeypatch.delenv("SMTP_HOST")
    assert _send() is False
    smtp.sendmail.assert_not_called()
