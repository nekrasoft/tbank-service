import os
import time
import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.cli.cron import _is_counterparty_due


class CronScheduleTests(unittest.TestCase):
    def test_evening_boundary_is_moscow_time(self):
        original_tz = os.environ.get("TZ")
        try:
            for server_tz in ("UTC", "Europe/Moscow"):
                os.environ["TZ"] = server_tz
                time.tzset()
                for schedule, day in (("daily", 5), ("monthly", 30),
                                      ("2weeks", 15), ("2weeks", 30),
                                      ("10days", 10), ("10days", 20), ("10days", 30),
                                      (None, 30), ("unknown", 30)):
                    for hour, minute, expected in ((19, 0, False), (21, 59, False), (22, 0, True)):
                        moscow = datetime(2026, 11, day, hour, minute, tzinfo=ZoneInfo("Europe/Moscow"))
                        for run_at in (moscow, moscow.astimezone(timezone.utc)):
                            with self.subTest(server_tz=server_tz, schedule=schedule, run_at=run_at):
                                self.assertEqual(_is_counterparty_due(schedule, run_at), expected)
        finally:
            if original_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = original_tz
            time.tzset()

    def test_non_billing_days_stay_excluded(self):
        for schedule in ("monthly", "2weeks", "10days"):
            with self.subTest(schedule=schedule):
                self.assertFalse(_is_counterparty_due(
                    schedule, datetime(2026, 11, 5, 22, tzinfo=ZoneInfo("Europe/Moscow"))
                ))

    def test_utc_previous_day_does_not_reopen_previous_month(self):
        run_at = datetime(2026, 11, 30, 21, 30, tzinfo=timezone.utc)
        for schedule in ("daily", "monthly", "2weeks", "10days"):
            with self.subTest(schedule=schedule):
                self.assertFalse(_is_counterparty_due(schedule, run_at))
