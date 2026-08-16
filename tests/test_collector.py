import asyncio
import logging

import pytest

from ble_prometheus_collector import collector


def test_prometheus_startup_failure_is_logged_and_raised(monkeypatch, caplog):
    error = OSError("address already in use")

    def fail_to_start(*args, **kwargs):
        raise error

    monkeypatch.setattr(collector, "start_http_server", fail_to_start)

    with (
        caplog.at_level(logging.ERROR),
        pytest.raises(OSError, match="address already in use"),
    ):
        asyncio.run(collector.main())

    assert "Failed to start Prometheus metrics server on port 8000" in caplog.text


def test_main_sync_does_not_hide_fatal_errors(monkeypatch, caplog):
    error = RuntimeError("startup failed")

    def fail(coroutine):
        coroutine.close()
        raise error

    monkeypatch.setattr(collector.asyncio, "run", fail)

    with (
        caplog.at_level(logging.CRITICAL),
        pytest.raises(RuntimeError, match="startup failed"),
    ):
        collector.main_sync()

    assert "Collector stopped due to an unhandled error" in caplog.text
