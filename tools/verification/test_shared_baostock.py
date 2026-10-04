from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import types

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "shared_baostock.py"
SPEC = importlib.util.spec_from_file_location("shared_baostock_test", SCRIPT)
SHARED = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SHARED)


def setup_guard(tmp_path, *, busy_until=0, calls=0, latest=None):
    clock = {"seconds": 0}
    lock = tmp_path / "shared.lock"
    lock.touch()
    class GuardStop(RuntimeError):
        pass
    class Base:
        def __init__(self, *, max_calls, **kwargs):
            self.max_calls = max_calls
            self.daily_cap = 40000
            self.run_calls = 0
            self.lock_path = lock
            self.ledger = {"calls": calls}
            self.acquired = False
        def today(self):
            return "2026-10-08"
        def _acquire_lock(self):
            if clock["seconds"] < busy_until:
                raise GuardStop("busy")
            self.acquired = True
        def __enter__(self):
            self._acquire_lock()
            return self
        def __exit__(self, *args):
            self.acquired = False
        def remaining(self):
            return min(self.max_calls - self.run_calls, self.daily_cap - self.ledger["calls"])
        def before_send(self):
            if self.remaining() <= 0:
                raise GuardStop("budget")
            self.ledger["calls"] += 1
            self.run_calls += 1
        def logout(self, client):
            self.before_send()
        def hook(self, sock_module):
            self._sock_module = sock_module
            self._orig_send = sock_module.send_msg
        def after_receive(self, reply):
            self.reply = reply
    module = types.SimpleNamespace(BaoStockGuard=Base, GuardStop=GuardStop,
                                   calls_on=lambda ledger, day: ledger["calls"])
    def sleep(seconds):
        clock["seconds"] += seconds
    now = lambda: datetime(2026, 10, 8, 18, tzinfo=timezone(timedelta(hours=8))) + timedelta(seconds=clock["seconds"])
    guard_type = SHARED.queued_guard_class(module, monotonic=lambda: clock["seconds"], sleep=sleep, now=now)
    guard = guard_type(max_calls=20, latest_start_local=latest, target_date="2026-10-08", wait_seconds=30)
    return guard, clock, GuardStop


def test_busy_lock_waits_without_spending_messages(tmp_path):
    guard, clock, _ = setup_guard(tmp_path, busy_until=10)
    with guard:
        assert clock["seconds"] == guard.waited_seconds == 10
        assert guard.run_calls == 0
        guard.before_send()
        assert guard.run_calls == 1


def test_queue_timeout_never_deletes_lock_or_logs_in(tmp_path):
    guard, clock, error = setup_guard(tmp_path, busy_until=100)
    with pytest.raises(error, match="lock_wait_timeout"):
        with guard:
            pytest.fail("lock was not acquired")
    assert clock["seconds"] == 30 and guard.run_calls == 0
    assert guard.lock_path.exists()


def test_budget_is_checked_again_after_wait(tmp_path):
    guard, _, error = setup_guard(tmp_path, calls=34990)
    with pytest.raises(error, match="shared_daily_budget_insufficient"):
        with guard:
            pytest.fail("budget was insufficient")
    assert not guard.acquired and guard.run_calls == 0


def test_latest_start_is_checked_after_wait(tmp_path):
    guard, _, error = setup_guard(tmp_path, busy_until=10, latest="18:00:05")
    with pytest.raises(error, match="latest_start_passed"):
        with guard:
            pytest.fail("late start was accepted")
    assert guard.run_calls == 0 and not guard.acquired


def test_logout_is_reserved_within_shared_cap(tmp_path):
    guard, _, error = setup_guard(tmp_path)
    with guard:
        guard.ledger["calls"] = 34999
        with pytest.raises(error, match="shared_soft_or_run_budget_stop"):
            guard.before_send()
        guard.logout(None)
        assert guard.ledger["calls"] == 35000


def test_stop_marker_before_lock_is_not_api_failure(tmp_path):
    guard, _, error = setup_guard(tmp_path, busy_until=100)
    guard.stop_file = tmp_path / "STOP"
    guard.stop_file.touch()
    with pytest.raises(error, match="operator_stop_before_lock"):
        with guard:
            pytest.fail("operator stopped the task")
    assert guard.run_calls == 0


@pytest.mark.parametrize("sent", [0, 1, 20])
def test_attempt_observes_actual_send_even_when_receive_fails(tmp_path, sent):
    guard, _, _ = setup_guard(tmp_path)
    attempts = []
    guard.on_first_send = lambda: attempts.append("sent")
    connection = types.SimpleNamespace(send=lambda payload: sent)
    context = types.SimpleNamespace(default_socket=connection)
    def sdk_send(message):
        context.default_socket.send(message.encode())
        return None
    socket_module = types.SimpleNamespace(context=context, send_msg=sdk_send)
    guard._orig_send = None
    with guard:
        guard.hook(socket_module)
        socket_module.send_msg("redacted")
        socket_module.send_msg("redacted")
    assert attempts == ([] if sent == 0 else ["sent"])


def test_connection_failure_before_send_never_creates_attempt(tmp_path):
    guard, _, _ = setup_guard(tmp_path)
    attempts = []
    guard.on_first_send = lambda: attempts.append("sent")
    def fail_send(payload):
        raise OSError("not connected")
    context = types.SimpleNamespace(default_socket=types.SimpleNamespace(send=fail_send))
    def sdk_send(message):
        try:
            context.default_socket.send(message.encode())
        except OSError:
            return None
    socket_module = types.SimpleNamespace(context=context, send_msg=sdk_send)
    guard._orig_send = None
    with guard:
        guard.hook(socket_module)
        socket_module.send_msg("redacted")
    assert attempts == []


def test_record_failure_blocks_next_send(tmp_path):
    guard, _, error = setup_guard(tmp_path)
    def fail_record():
        raise OSError("disk full")
    guard.on_first_send = fail_record
    context = types.SimpleNamespace(default_socket=types.SimpleNamespace(send=lambda payload: len(payload)))
    def sdk_send(message):
        try:
            context.default_socket.send(message.encode())
        except OSError:
            return None
    socket_module = types.SimpleNamespace(context=context, send_msg=sdk_send)
    guard._orig_send = None
    with guard:
        guard.hook(socket_module)
        with pytest.raises(error, match="first_send_record_failed"):
            socket_module.send_msg("redacted")
        with pytest.raises(error, match="first_send_record_failed"):
            socket_module.send_msg("redacted")
    assert guard.run_calls == 1
