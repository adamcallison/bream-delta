from datetime import datetime
from functools import cached_property
from typing import cast

from bream.core import Batch, BatchRequest, Pathlike, Source
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_DELTA_FORMAT = "delta"
_HISTORY_VERSION_COL = "version"
_HISTORY_TIMESTAMP_COL = "timestamp"
_COMMIT_VERSION_COL = "_commit_version"
_COMMIT_TIMESTAMP_COL = "_commit_timestamp"
_CHANGE_TYPE_COL = "_change_type"
_CHANGE_TYPE_INSERT = "insert"


class DeltaTableChangeFeedSource(Source):
    def __init__(
        self,
        *,
        spark: SparkSession,
        path: Pathlike,
        commits_per_batch: int,
        name: str | None = None,
        start_from: int | datetime | None = None,
    ) -> None:
        self._spark = spark
        self._path = path
        self._commits_per_batch = commits_per_batch
        self._start_from = start_from
        self._start_from_version: int | None

        self.name = name if name is not None else path.name

    @cached_property
    def _start_from_version(self) -> int | None:
        start_from_version: int | None
        if isinstance(self._start_from, datetime):
            _, start_from_version = self._get_timestamp_version_pair(
                timestamp=self._start_from,
            )
        else:
            start_from_version = self._start_from
        return start_from_version

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
        _, current_version = self._get_timestamp_version_pair()
        if current_version < starting_version:
            return None

        ending_version = (
            read_to if read_to is not None else starting_version + (self._commits_per_batch - 1)
        )
        df = (
            self._spark.read.format(_DELTA_FORMAT)
            .options(
                readChangeFeed=True,
                startingVersion=starting_version,
                endingVersion=ending_version,
            )
            .load(str(self._path))
        )
        return df, min(current_version, ending_version)

    def _read_initial_snapshot(self, *, at_version: int | None) -> tuple[DataFrame, int]:
        at_timestamp, at_version = self._get_timestamp_version_pair(version=at_version)
        df = (
            self._spark.read.format(_DELTA_FORMAT)
            .options(versionAsOf=at_version)
            .load(str(self._path))
        ).select(
            "*",
            F.lit(_CHANGE_TYPE_INSERT).alias(_CHANGE_TYPE_COL),
            F.lit(at_version).alias(_COMMIT_VERSION_COL),
            F.lit(at_timestamp).alias(_COMMIT_TIMESTAMP_COL),
        )
        return df, at_version

    def _get_timestamp_version_pair(
        self,
        *,
        version: int | None = None,
        timestamp: datetime | None = None,
    ) -> tuple[datetime, int]:
        if version is not None and timestamp is not None:
            msg = "Cannot specify both version and timestamp."
            raise ValueError(msg)

        delta_table = DeltaTable.forPath(self._spark, str(self._path))
        delta_history_df = delta_table.history().select(
            _HISTORY_VERSION_COL,
            _HISTORY_TIMESTAMP_COL,
        )
        latest_row = delta_history_df.collect()[0]
        if version is None and timestamp is None:
            specified_row = latest_row
        elif version is not None:
            specified_row = delta_history_df.where(
                F.col(_HISTORY_VERSION_COL) == version,
            ).collect()[0]
        else:  # timestamp is not None:
            specified_row = (
                delta_history_df.where(F.col(_HISTORY_TIMESTAMP_COL) >= F.lit(timestamp))
                .orderBy(F.col(_HISTORY_TIMESTAMP_COL).asc())
                .limit(1)
                .collect()[0]
            )

        return specified_row.timestamp, specified_row.version
