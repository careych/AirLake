"""Boundary for a future read-only Athena executor; no SDK/session is created."""

from alerts.gold import InvalidAlert


class AthenaGoldSource:
    """The injected executor must pin one Iceberg snapshot and bound its query.

    It returns enriched Gold rows: hourly facts joined to verified dimension names,
    explicit units/method, source table and decimal-string snapshot ID. Implementing
    live query submission, result pagination and snapshot pinning remains deployment
    work; this class cannot silently fall back to synthetic or latest unpinned rows.
    """

    def __init__(self, executor=None):
        self.executor = executor

    def read(self):
        if self.executor is None:
            raise InvalidAlert("Live Athena executor is not configured")
        return self.executor.read_pinned_hourly_rows()
