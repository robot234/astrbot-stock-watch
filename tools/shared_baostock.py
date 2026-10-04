"""Queue Stock Watch callers on the existing owner-host transport ledger."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import time


CHINA = timezone(timedelta(hours=8))
OWNER = Path("/home/pi/apps/stock-fund-fetch-20260929")
HARD_CAP = 40000
SOFT_CAP = 35000
LOCK_WAIT_SECONDS = 7200


class SendObserver:
    def __init__(self, connection, notify):
        self.connection = connection
        self.notify = notify

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def send(self, payload, *args, **kwargs):
        sent = self.connection.send(payload, *args, **kwargs)
        if sent > 0:
            self.notify()
        return sent


def _load_owner(root: Path):
    spec = importlib.util.spec_from_file_location("stock_watch_owner_guard", root / "baostock_guard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def queued_guard_class(module, *, monotonic=time.monotonic, sleep=time.sleep,
                       now=lambda: datetime.now(CHINA)):
    class QueuedGuard(module.BaoStockGuard):
        def __init__(self, *args, wait_seconds=LOCK_WAIT_SECONDS, stop_file=None,
                     target_date=None, latest_start_local=None, work_seconds=7200,
                     on_first_send=None, before_send_check=None, **kwargs):
            super().__init__(*args, **kwargs)
            self.daily_cap = min(self.daily_cap, HARD_CAP)
            self.wait_seconds = wait_seconds
            self.stop_file = Path(stop_file) if stop_file else None
            self.target_date = target_date
            self.latest_start_local = latest_start_local
            self.work_seconds = work_seconds
            self.work_deadline = None
            self.cleanup = False
            self.waited_seconds = 0
            self.on_first_send = on_first_send
            self.first_send_seen = False
            self.observation_error = None
            self.before_send_check = before_send_check

        def observe_send(self):
            if not self.first_send_seen:
                self.first_send_seen = True
                try:
                    if self.on_first_send is not None:
                        self.on_first_send()
                except Exception as exc:
                    self.observation_error = exc
                    raise

        def hook(self, sock_module=None):
            if self._orig_send is not None:
                return
            if sock_module is None:
                import baostock.util.socketutil as sock_module
            super().hook(sock_module)

            def guarded_send(message):
                self.before_send()
                context = sock_module.context
                connection = getattr(context, "default_socket", None)
                if connection is not None and not isinstance(connection, SendObserver):
                    context.default_socket = SendObserver(connection, self.observe_send)
                reply = self._orig_send(message)
                if self.observation_error is not None:
                    raise module.GuardStop("first_send_record_failed") from self.observation_error
                self.after_receive(reply)
                return reply

            sock_module.send_msg = guarded_send

        def stopped(self):
            return self.stop_file is not None and self.stop_file.exists()

        def _acquire_lock(self):
            started = monotonic()
            deadline = started + self.wait_seconds
            while True:
                if self.stopped():
                    raise module.GuardStop("operator_stop_before_lock")
                try:
                    super()._acquire_lock()
                    self.waited_seconds = monotonic() - started
                    return
                except module.GuardStop:
                    if not self.lock_path.exists():
                        raise
                    if monotonic() >= deadline:
                        raise module.GuardStop("lock_wait_timeout") from None
                    sleep(min(5, max(0, deadline - monotonic())))

        def __enter__(self):
            super().__enter__()
            try:
                self.work_deadline = monotonic() + self.work_seconds
                local = now().astimezone(CHINA)
                if self.target_date and local.date().isoformat() != self.target_date:
                    raise module.GuardStop("target_date_changed_while_waiting")
                if self.latest_start_local and local.strftime("%H:%M:%S") > self.latest_start_local:
                    raise module.GuardStop("latest_start_passed_while_waiting")
                if module.calls_on(self.ledger, self.today()) + self.max_calls > SOFT_CAP:
                    raise module.GuardStop("shared_daily_budget_insufficient")
                return self
            except Exception:
                super().__exit__(None, None, None)
                raise

        def before_send(self):
            if self.observation_error is not None:
                raise module.GuardStop("first_send_record_failed") from self.observation_error
            if not self.cleanup:
                if self.before_send_check is not None:
                    self.before_send_check()
                if self.stopped():
                    raise module.GuardStop("operator_stop")
                if monotonic() >= self.work_deadline:
                    raise module.GuardStop("work_deadline_exceeded")
                if self.target_date and now().astimezone(CHINA).date().isoformat() != self.target_date:
                    raise module.GuardStop("target_date_changed")
                if module.calls_on(self.ledger, self.today()) >= SOFT_CAP - 1 or self.remaining() <= 1:
                    raise module.GuardStop("shared_soft_or_run_budget_stop")
            elif module.calls_on(self.ledger, self.today()) >= SOFT_CAP:
                raise module.GuardStop("shared_cleanup_budget_stop")
            super().before_send()

        def logout(self, client):
            self.cleanup = True
            try:
                super().logout(client)
            finally:
                self.cleanup = False

    return QueuedGuard


@contextmanager
def owner_session(*, purpose: str, max_calls: int, guard_dir: Path = OWNER,
                  wait_seconds: int = LOCK_WAIT_SECONDS, stop_file=None, target_date=None,
                  latest_start_local=None, work_seconds=7200, on_first_send=None,
                  before_send_check=None):
    module = _load_owner(Path(guard_dir))
    guard_type = queued_guard_class(module)
    with guard_type(purpose=purpose, max_calls=max_calls, wait_seconds=wait_seconds,
                    stop_file=stop_file, target_date=target_date, latest_start_local=latest_start_local,
                    work_seconds=work_seconds, on_first_send=on_first_send,
                    before_send_check=before_send_check,
                    rules_path=Path(guard_dir) / "baostock_access_rules.json",
                    ledger_path=Path(guard_dir) / "baostock_request_ledger.json",
                    lock_path=Path(guard_dir) / "baostock_guard.lock") as guard:
        if not module.release_status([module.detect_ipv4()])["ok"]:
            raise module.GuardStop("official_blacklist_check_unverified")
        yield guard
