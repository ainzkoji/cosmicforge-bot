"""A read-only code tree (container images mount it read-only) must not stop the bot backend at import."""
from __future__ import annotations

import importlib
import logging


def test_backtesting_router_imports_when_its_log_file_cannot_be_opened(monkeypatch):
    import app.api.backtesting as module

    file_handler = logging.FileHandler
    log = logging.getLogger(module.__name__)
    for h in list(log.handlers):                  # handlers from the first import of the module
        if isinstance(h, file_handler):
            log.removeHandler(h)
            h.close()

    def refuse(*args, **kwargs):
        raise OSError(30, "Read-only file system")

    monkeypatch.setattr(logging, "FileHandler", refuse)
    reloaded = importlib.reload(module)
    assert reloaded.router is not None
    assert not [h for h in reloaded.logger.handlers if isinstance(h, file_handler)]
