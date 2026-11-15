"""Generates the Flink SQL job for the per-transaction features from the definitions
(ADR-003): card features, and merchant features from the same topic.

Flink allows one OVER window per SELECT, so features are grouped by window: one view
per window size, one for the previous card-present transaction, and one per merchant
window, joined back to each transaction with zero-width interval joins. The same
statements run in the equality test against a file source and in the deployed job
against Kafka.

Event time is each event's change time: a creation's event_ts, and for an update the
simulated time its status changed (status_event_ts). Creations and status changes then
arrive in event-time order on one topic, so a reversal is never late.
"""

from dataclasses import dataclass

from features.model import Definitions, Feature

TOPIC = "pg.public.transactions"
SINK_TOPIC = "features.card"
LATE_TOPIC = "dlq.late_events"
EARTH_RADIUS_KM = 6371.0088

# Transaction fields the card features read, as Debezium's Avro converter writes them.
TXN_FIELDS = {
    "transaction_id": "BIGINT",
    "card_id": "BIGINT",
    "merchant_id": "BIGINT",
    "amount": "DECIMAL(12, 2)",
    "channel": "STRING",
    "lat": "DOUBLE",
    "lon": "DOUBLE",
    "event_ts": "STRING",
    "status": "STRING",
    "status_event_ts": "STRING",
}
DTYPES = {
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "decimal(18,2)": "DECIMAL(18, 2)",
}
UNITS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY"}
PREVIOUS_KINDS = ("distance_from_previous", "minutes_since_previous")

# Debezium writes timestamptz as ISO-8601 text with 0 to 6 fraction digits, always UTC.
# Epoch milliseconds, truncated (not rounded), as the definitions require. The change
# time: authorization for a creation, the status change for an update.
_TS = (
    "CASE WHEN `op` = 'c' THEN `after`.event_ts "
    "ELSE COALESCE(`after`.status_event_ts, `after`.event_ts, `before`.event_ts) END"
)
EVENT_MS = (
    f"UNIX_TIMESTAMP(SUBSTRING({_TS} FROM 1 FOR 19), 'yyyy-MM-dd''T''HH:mm:ss') * 1000 + "
    f"CAST(SUBSTRING(CONCAT(COALESCE(REGEXP_EXTRACT({_TS}, '\\.([0-9]+)', 1), ''), '000') "
    "FROM 1 FOR 3) AS INT)"
)


def _millis(window: str) -> int:
    seconds = {"s": 1, "m": 60, "h": 3600, "d": 86400}[window[-1]]
    return int(window[:-1]) * seconds * 1000


def interval(window: str) -> str:
    return f"INTERVAL '{window[:-1]}' {UNITS[window[-1]]}"


def haversine(lat1: str, lon1: str, lat2: str, lon2: str) -> str:
    return (
        f"2 * {EARTH_RADIUS_KM} * ASIN(SQRT("
        f"POWER(SIN(RADIANS({lat2} - {lat1}) / 2), 2) + "
        f"COS(RADIANS({lat1})) * COS(RADIANS({lat2})) * "
        f"POWER(SIN(RADIANS({lon2} - {lon1}) / 2), 2)))"
    )


def _view(window: str) -> str:
    return f"card_w_{window}"


@dataclass(frozen=True)
class CardJob:
    """The card feature job, independent of where events come from and go to."""

    defs: Definitions

    @property
    def card_features(self) -> tuple[Feature, ...]:
        return self.defs.for_entity("card")

    @property
    def merchant_features(self) -> tuple[Feature, ...]:
        return self.defs.for_entity("merchant")

    @property
    def features(self) -> tuple[Feature, ...]:
        """Everything this job emits per transaction, in definition order."""
        return tuple(f for f in self.defs.features if f.entity in ("card", "merchant"))

    def windows(self) -> list[str]:
        seen: list[str] = []
        for f in self.card_features:
            if f.window and f.window not in seen:
                seen.append(f.window)
        return seen

    def previous_filter(self) -> str | None:
        filters = {f.filter for f in self.card_features if f.kind in PREVIOUS_KINDS}
        if len(filters) > 1:
            raise ValueError(f"previous-transaction features must share one filter: {filters}")
        return filters.pop() if filters else None

    def source_ddl(self, options: dict[str, str]) -> str:
        row = ", ".join(f"`{name}` {t}" for name, t in TXN_FIELDS.items())
        return f"""CREATE TEMPORARY TABLE pg_transactions (
    `op` STRING,
    `before` ROW<{row}>,
    `after` ROW<{row}>,
    `source` ROW<`ts_ms` BIGINT>,
    event_ms AS {EVENT_MS},
    event_time AS TO_TIMESTAMP_LTZ({EVENT_MS}, 3),
    WATERMARK FOR event_time AS event_time - {interval(str(self.defs.semantics["watermark"]))}
) WITH (
{_options(options)}
)"""

    def views(self) -> list[str]:
        fields = ", ".join(f"`after`.{name} AS {name}" for name in TXN_FIELDS if name != "event_ts")
        on_time = (
            "CURRENT_WATERMARK(event_time) IS NULL OR event_time >= CURRENT_WATERMARK(event_time)"
        )
        views = [
            # Transactions at creation (FR-F1), minus events already behind the watermark.
            f"CREATE TEMPORARY VIEW card_txn AS SELECT {fields}, "
            "`source`.ts_ms AS commit_ms, event_ms, event_time "
            f"FROM pg_transactions WHERE op = 'c' AND ({on_time})",
            "CREATE TEMPORARY VIEW card_late AS SELECT `after`.transaction_id AS transaction_id, "
            "`after`.card_id AS card_id, event_ms, "
            "CAST(CURRENT_WATERMARK(event_time) AS STRING) AS watermark_at "
            "FROM pg_transactions WHERE op = 'c' AND event_time < CURRENT_WATERMARK(event_time)",
        ]
        for window in self.windows():
            over = (
                "OVER (PARTITION BY card_id ORDER BY event_time "
                f"RANGE BETWEEN {interval(window)} PRECEDING AND CURRENT ROW)"
            )
            cols: list[str] = []
            for f in self.card_features:
                if f.window != window:
                    continue
                if f.kind == "window_count":
                    cols.append(f"COUNT(*) {over} AS {f.name}")
                elif f.kind == "window_sum":
                    cols.append(f"CAST(SUM({f.field}) {over} AS DECIMAL(18, 2)) AS {f.name}")
                elif f.kind == "window_count_distinct":
                    cols.append(f"COUNT(DISTINCT {f.field}) {over} AS {f.name}")
                elif f.kind == "ratio_to_prior_mean":
                    cols.append(f"COUNT(*) {over} AS {f.name}__n")
                    cols.append(f"SUM({f.field}) {over} AS {f.name}__sum")
                else:
                    raise NotImplementedError(f"{f.name}: kind {f.kind}")
            # Flink 2.1.3's planner merges OVER aggregates that differ only in their RANGE
            # bound: a 24h and a 30d window with the same aggregates both get one bound.
            # A window-specific filter (true for every event) keeps their plans distinct.
            # tests/features/test_flink_equality.py fails if this ever stops working.
            views.append(
                f"CREATE TEMPORARY VIEW {_view(window)} AS SELECT transaction_id, event_time, "
                f"{', '.join(cols)} FROM card_txn WHERE event_ms > -{_millis(window)}"
            )
        if (flt := self.previous_filter()) is not None:
            over = (
                "OVER (PARTITION BY card_id ORDER BY event_time "
                "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"
            )
            # Over the current row and the one before it, SUM(x) - x is the previous x.
            views.append(
                "CREATE TEMPORARY VIEW card_prev AS SELECT transaction_id, event_time, event_ms, "
                f"lat, lon, COUNT(*) {over} AS n, SUM(lat) {over} AS lat_sum, "
                f"SUM(lon) {over} AS lon_sum, SUM(event_ms) {over} AS ms_sum "
                f"FROM card_txn WHERE {flt}"
            )
        views += self._merchant_views(on_time)
        return views

    def _merchant_views(self, on_time: str) -> list[str]:
        """Per merchant window: creations and status changes counted over one OVER window."""
        views: list[str] = []
        for f in self.merchant_features:
            if f.kind != "status_rate":
                raise NotImplementedError(f"{f.name}: kind {f.kind}")
            name, window = f.name, str(f.window)
            views.append(
                f"CREATE TEMPORARY VIEW {name}_events AS SELECT `after`.transaction_id AS "
                "transaction_id, `after`.merchant_id AS merchant_id, event_ms, event_time, "
                "CASE WHEN op = 'c' THEN 1 ELSE 0 END AS created, "
                "CASE WHEN op = 'u' THEN 1 ELSE 0 END AS changed "
                f"FROM pg_transactions WHERE (op = 'c' OR (op = 'u' "
                f"AND `after`.status = '{f.status}' AND `before`.status <> '{f.status}' "
                f"AND `after`.{f.status_time} IS NOT NULL)) AND ({on_time})"
            )
            over = (
                "OVER (PARTITION BY merchant_id ORDER BY event_time "
                f"RANGE BETWEEN {interval(window)} PRECEDING AND CURRENT ROW)"
            )
            views.append(
                f"CREATE TEMPORARY VIEW {name}_w AS SELECT transaction_id, event_time, created, "
                f"SUM(created) {over} AS n_created, SUM(changed) {over} AS n_changed "
                f"FROM {name}_events"
            )
        return views

    def _expression(self, f: Feature) -> str:
        if f.kind in ("window_count", "window_sum", "window_count_distinct"):
            return f"{_view(str(f.window))}.{f.name}"
        if f.kind == "ratio_to_prior_mean":
            v = _view(str(f.window))
            return (
                f"CASE WHEN {v}.{f.name}__n > 1 THEN CAST(t.{f.field} AS DOUBLE) / "
                f"(CAST({v}.{f.name}__sum - t.{f.field} AS DOUBLE) / ({v}.{f.name}__n - 1)) END"
            )
        if f.kind == "distance_from_previous":
            prev = haversine("(p.lat_sum - p.lat)", "(p.lon_sum - p.lon)", "p.lat", "p.lon")
            return f"CASE WHEN p.n = 2 THEN {prev} END"
        if f.kind == "status_rate":
            m = f"{f.name}_w"
            return f"CAST({m}.n_changed AS DOUBLE) / {m}.n_created"
        if f.kind == "minutes_since_previous":
            return (
                "CASE WHEN p.n = 2 "
                "THEN CAST(t.event_ms - (p.ms_sum - p.event_ms) AS DOUBLE) / 60000.0 END"
            )
        raise NotImplementedError(f"{f.name}: kind {f.kind}")

    def select(self) -> str:
        """One row per on-time transaction: every card feature."""
        cols = ", ".join(
            f"CAST({self._expression(f)} AS {DTYPES[f.dtype]}) AS {f.name}" for f in self.features
        )
        joins = "".join(
            f" JOIN {_view(w)} ON {_view(w)}.transaction_id = t.transaction_id "
            f"AND {_view(w)}.event_time BETWEEN t.event_time AND t.event_time"
            for w in self.windows()
        )
        for f in self.merchant_features:
            m = f"{f.name}_w"
            joins += (
                f" JOIN {m} ON {m}.transaction_id = t.transaction_id AND {m}.created = 1 "
                f"AND {m}.event_time BETWEEN t.event_time AND t.event_time"
            )
        if self.previous_filter() is not None:
            joins += (
                " LEFT JOIN card_prev p ON p.transaction_id = t.transaction_id "
                "AND p.event_time BETWEEN t.event_time AND t.event_time"
            )
        return (
            f"SELECT t.transaction_id, t.card_id, t.merchant_id, t.event_ms, t.commit_ms, {cols} "
            f"FROM card_txn t{joins}"
        )

    def sink_columns(self) -> str:
        cols = ", ".join(f"{f.name} {DTYPES[f.dtype]}" for f in self.features)
        return (
            "card_key STRING, transaction_id BIGINT, card_id BIGINT, merchant_id BIGINT, "
            "event_ms BIGINT, "
            f"commit_ms BIGINT, {cols}, definitions_version INT, computed_at STRING"
        )

    def job(self, bootstrap: str, registry: str) -> str:
        source = {
            "connector": "kafka",
            "topic": TOPIC,
            "properties.bootstrap.servers": bootstrap,
            "properties.group.id": "features-card",
            # A fresh submission replays the topic from the start to rebuild window state:
            # the state is the history. Resuming from committed offsets without it would
            # compute every window from empty. (Failover inside Flink restores checkpoints.)
            "scan.startup.mode": "earliest-offset",
            "value.format": "avro-confluent",
            "value.avro-confluent.url": registry,
        }
        sink = {
            "connector": "kafka",
            "topic": SINK_TOPIC,
            "properties.bootstrap.servers": bootstrap,
            "key.format": "raw",
            "key.fields": "card_key",
            "value.format": "avro-confluent",
            "value.avro-confluent.url": registry,
            "value.fields-include": "EXCEPT_KEY",
            # At-least-once: a transactional sink would hold features back until the next
            # checkpoint, far beyond the decision latency SLO. Consumers key on transaction_id.
            "sink.delivery-guarantee": "at-least-once",
        }
        late = {
            "connector": "kafka",
            "topic": LATE_TOPIC,
            "properties.bootstrap.servers": bootstrap,
            "format": "json",
            "sink.delivery-guarantee": "at-least-once",
        }
        stamp = (
            "DATE_FORMAT(CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)), "
            "'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')"
        )
        statements = [
            "SET 'pipeline.name' = 'features-card'",
            "SET 'execution.runtime-mode' = 'streaming'",
            "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE'",
            "SET 'table.local-time-zone' = 'UTC'",
            self.source_ddl(source),
            *self.views(),
            f"CREATE TEMPORARY TABLE features_card ({self.sink_columns()}) "
            f"WITH (\n{_options(sink)}\n)",
            "CREATE TEMPORARY TABLE late_events (source_topic STRING, transaction_id BIGINT, "
            "card_id BIGINT, event_ms BIGINT, watermark_at STRING, detected_at STRING) "
            f"WITH (\n{_options(late)}\n)",
        ]
        names = ", ".join(f.name for f in self.features)
        body = (
            "EXECUTE STATEMENT SET\nBEGIN\n"
            "INSERT INTO features_card SELECT CAST(card_id AS STRING), transaction_id, card_id, "
            f"merchant_id, event_ms, commit_ms, {names}, {self.defs.version}, {stamp} "
            f"FROM ({self.select()});\n"
            f"INSERT INTO late_events SELECT '{TOPIC}', transaction_id, card_id, event_ms, "
            f"watermark_at, {stamp} FROM card_late;\n"
            "END;\n"
        )
        header = (
            "-- Generated by `uv run features flink` from features/definitions.yml. "
            "Do not edit by hand.\n"
            "-- Card and merchant features (FR-F1, FR-F2, FR-F6): one record per transaction on\n"
            "-- features.card; transactions behind the 30 s watermark go to dlq.late_events.\n\n"
        )
        return header + ";\n\n".join(statements) + ";\n\n" + body


def _options(options: dict[str, str]) -> str:
    return ",\n".join(f"    '{k}' = '{v}'" for k, v in options.items())
