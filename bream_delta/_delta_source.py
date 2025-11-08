from datetime import datetime
from functools import cached_property
from typing import NamedTuple, cast

from bream.core import Batch, BatchRequest, Pathlike, Source
from delta import DeltaTable
from pyspark.sql import Column, DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_DELTA_FORMAT = "delta"
_HISTORY_VERSION_COL = "version"
_HISTORY_TIMESTAMP_COL = "timestamp"
_HISTORY_OPERATION_COL = "operation"
_COMMIT_VERSION_COL = "_commit_version"
_COMMIT_TIMESTAMP_COL = "_commit_timestamp"
_CHANGE_TYPE_COL = "_change_type"
_CHANGE_TYPE_INSERT = "insert"

_NON_DATA_OPERATION_TYPES = ["OPTIMIZE", "VACUUM"]


class _DeltaHistoryRow(NamedTuple):
    version: int
    timestamp: datetime
    operation: str


class DeltaTableChangeFeedSource(Source):
    def __init__(  # noqa: PLR0913
        self,
        *,
        spark: SparkSession,
        path: Pathlike,
        commits_per_batch: int,
        start_from: int | datetime | None = None,
        ignore_non_data_commits: bool = True,
        name: str | None = None,
    ) -> None:
        self._spark = spark
        self._path = path
        self._ensure_table()
        self._commits_per_batch = commits_per_batch
        self._start_from = start_from
        self._ignore_non_data_commits = ignore_non_data_commits

        self.name = name if name is not None else path.name

    @cached_property
    def _start_from_version(self) -> int | None:
        start_from_version: int | None
        if isinstance(self._start_from, datetime):
            # TODO: give proper error in case where starting version doesn't exist
            start_from_version = self._get_delta_history(read_from=self._start_from)[0].version
        else:
            start_from_version = self._start_from
        return start_from_version

    def _ensure_table(self) -> None:
        if not DeltaTable.isDeltaTable(self._spark, str(self._path)):
            msg = f"'{self._path}' is not a delta table"
            raise ValueError(msg)

    def read(self, batch_request: BatchRequest) -> Batch | None:
        df: DataFrame | None
        read_from_after = cast("int | None", batch_request.read_from_after)
        read_to = cast("int | None", batch_request.read_to)

        starting_version = (
            read_from_after + 1 if read_from_after is not None else self._start_from_version
        )

        if starting_version is None:
            df, read_to_actual = self._read_initial_snapshot(at_version=read_to)
        else:
            maybe_df_and_read_to_actual = self._read_change_feed_batch(
                starting_version=starting_version,
                read_to=read_to,
            )
            if maybe_df_and_read_to_actual is None:
                return None
            df, read_to_actual = maybe_df_and_read_to_actual
        return Batch(data=df, read_to=read_to_actual)

    def _read_change_feed_batch(
        self,
        *,
        starting_version: int,
        read_to: int | None,
    ) -> tuple[DataFrame, int] | None:
        if read_to is not None:
            ending_version = read_to
        else:
            delta_history = self._get_delta_history(read_from=starting_version)
            if self._ignore_non_data_commits:
                delta_history = [
                    r for r in delta_history if r.operation not in _NON_DATA_OPERATION_TYPES
                ]
            if not delta_history:
                return None
            ending_version = delta_history[: self._commits_per_batch][-1].version

        df = (
            self._spark.read.format(_DELTA_FORMAT)
            .options(
                readChangeFeed=True,
                startingVersion=starting_version,
                endingVersion=ending_version,
            )
            .load(str(self._path))
        )
        return df, ending_version

    def _read_initial_snapshot(self, *, at_version: int | None) -> tuple[DataFrame, int]:
        delta_history_row = self._get_delta_history(read_from=at_version)[0]
        at_timestamp, at_version = delta_history_row.timestamp, delta_history_row.version
        df = (
            self._spark.read.format(_DELTA_FORMAT)
            .options(versionAsOf=at_version)
            .load(str(self._path))
        ).select(
            "*",
            _nullable(F.lit(_CHANGE_TYPE_INSERT)).alias(_CHANGE_TYPE_COL),
            _nullable(F.lit(at_version)).cast(T.LongType()).alias(_COMMIT_VERSION_COL),
            _nullable(F.lit(at_timestamp)).alias(_COMMIT_TIMESTAMP_COL),
        )
        return df, at_version

    def _get_delta_history(
        self,
        *,
        read_from: int | datetime | None = None,
    ) -> list[_DeltaHistoryRow]:
        delta_table = DeltaTable.forPath(self._spark, str(self._path))
        delta_history_df = delta_table.history().select(
            _HISTORY_VERSION_COL,
            _HISTORY_TIMESTAMP_COL,
            _HISTORY_OPERATION_COL,
        )
        if read_from is None:
            rows = [cast("Row", delta_history_df.first())]
        elif isinstance(read_from, int):
            rows = delta_history_df.where(F.col(_HISTORY_VERSION_COL) >= read_from).collect()
        else:  # timestamp
            rows = delta_history_df.where(F.col(_HISTORY_TIMESTAMP_COL) >= read_from).collect()

        return sorted(
            [_DeltaHistoryRow(r.version, r.timestamp, r.operation) for r in rows],
            key=lambda r: r.version,
        )


def _nullable(col: Column) -> Column:
    return F.when(F.lit(True), col)  # noqa: FBT003
