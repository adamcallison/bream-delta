from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

import pytest
from bream.core import BatchRequest, Pathlike
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from bream_delta._delta_source import DeltaTableChangeFeedSource


@dataclass
class _DeltaTableMetadata:
    path: Pathlike
    version_to_timestamp: Mapping[int, datetime]


def df_to_sorted_dicts(df, sort_by):
    rows = [row.asDict(recursive=True) for row in df.collect()]
    return sorted(rows, key=lambda x: x[sort_by])


class TestDeltaTableChangeFeedSource:
    """Integration tests for DeltaTableChangeFeedSource."""

    @pytest.fixture(scope="class")
    def sample_delta_table(self, spark, tmp_path_factory) -> _DeltaTableMetadata:
        num_versions = 4
        table_path = tmp_path_factory.mktemp("table")
        schema = StructType(
            [
                StructField("id", IntegerType(), False),  # noqa: FBT003
                StructField("name", StringType(), True),  # noqa: FBT003
            ],
        )

        for i in range(num_versions):
            df = spark.createDataFrame([(i, f"Person{i}")], schema)
            df.write.format("delta").option("delta.enableChangeDataFeed", "true").mode(
                "append",
            ).save(str(table_path))

        full_table = (
            spark.read.format("delta")
            .options(readChangeFeed=True, startingVersion=0)
            .load(str(table_path))
        )
        version_to_timestamp = {
            r._commit_version: r._commit_timestamp  # noqa: SLF001
            for r in full_table.select("_commit_version", "_commit_timestamp").distinct().collect()
        }

        return _DeltaTableMetadata(
            path=table_path,
            version_to_timestamp=version_to_timestamp,
        )

    def test_read_initial_snapshot_without_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=2,
        )

        batch_request = BatchRequest(read_from_after=None, read_to=None)
        batch = source.read(batch_request)

        expected_read_to = max(sample_delta_table.version_to_timestamp.keys())
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": expected_read_to,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[expected_read_to],
            }
            for j in range(expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_read_initial_snapshot_with_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        read_to = 1
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=2,
        )

        batch_request = BatchRequest(read_from_after=None, read_to=read_to)
        batch = source.read(batch_request)

        expected_read_to = read_to
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": expected_read_to,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[expected_read_to],
            }
            for j in range(expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_start_from_version_without_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        start_from_version = 1
        commits_per_batch = 2
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
            start_from=start_from_version,
        )

        batch_request = BatchRequest(read_from_after=None, read_to=None)
        batch = source.read(batch_request)

        expected_read_to = start_from_version + commits_per_batch - 1
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(start_from_version, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_start_from_version_with_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        read_to = 1
        start_from_version = 1
        commits_per_batch = 2
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
            start_from=start_from_version,
        )

        batch_request = BatchRequest(read_from_after=None, read_to=read_to)
        batch = source.read(batch_request)

        expected_read_to = read_to
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(start_from_version, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_start_from_timestamp_without_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        start_from_version = 1
        start_from_timestamp = sample_delta_table.version_to_timestamp[start_from_version]
        commits_per_batch = 2
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
            start_from=start_from_timestamp,
        )

        batch_request = BatchRequest(read_from_after=None, read_to=None)
        batch = source.read(batch_request)

        expected_read_to = start_from_version + commits_per_batch - 1
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(start_from_version, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_start_from_timestamp_with_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        read_to = 1
        start_from_version = 1
        start_from_timestamp = sample_delta_table.version_to_timestamp[start_from_version]
        commits_per_batch = 2
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
            start_from=start_from_timestamp,
        )

        batch_request = BatchRequest(read_from_after=None, read_to=read_to)
        batch = source.read(batch_request)

        expected_read_to = read_to
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(start_from_version, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_read_batch_without_unspecified_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        read_from_after = 0
        commits_per_batch = 2
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
        )

        batch_request = BatchRequest(read_from_after=read_from_after, read_to=None)
        batch = source.read(batch_request)

        expected_read_to = read_from_after + commits_per_batch
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(read_from_after + 1, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_read_batch_with_read_to(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        read_from_after = 0
        read_to = 1
        commits_per_batch = 2
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
        )

        batch_request = BatchRequest(read_from_after=read_from_after, read_to=read_to)
        batch = source.read(batch_request)

        assert batch is not None
        assert batch.read_to == read_to

        expected_read_to = read_to
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(read_from_after + 1, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_read_batch_with_large_commits_per_batch(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        read_from_after = 0
        commits_per_batch = 100
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
        )

        batch_request = BatchRequest(read_from_after=read_from_after, read_to=None)
        batch = source.read(batch_request)

        expected_read_to = max(sample_delta_table.version_to_timestamp.keys())
        expected_rows = [
            {
                "id": j,
                "name": f"Person{j}",
                "_change_type": "insert",
                "_commit_version": j,
                "_commit_timestamp": sample_delta_table.version_to_timestamp[j],
            }
            for j in range(read_from_after + 1, expected_read_to + 1)
        ]
        assert batch is not None
        assert batch.read_to == expected_read_to
        rows = df_to_sorted_dicts(batch.data, sort_by="id")
        assert rows == expected_rows

    def test_read_batch_returns_none_when_no_new_data(
        self,
        spark,
        sample_delta_table: _DeltaTableMetadata,
    ):
        commits_per_batch = 2
        read_from_after = max(sample_delta_table.version_to_timestamp.keys())
        source = DeltaTableChangeFeedSource(
            spark=spark,
            path=sample_delta_table.path,
            commits_per_batch=commits_per_batch,
        )

        batch_request = BatchRequest(read_from_after=read_from_after, read_to=None)
        batch = source.read(batch_request)

        assert batch is None

    def test_instantiation_raises_when_path_is_not_delta_table(
        self,
        spark,
        tmp_path,
    ):
        non_delta_path = tmp_path / "not_a_delta_table"
        non_delta_path.mkdir()

        with pytest.raises(ValueError, match=r"'.*' is not a delta table"):
            DeltaTableChangeFeedSource(
                spark=spark,
                path=non_delta_path,
                commits_per_batch=2,
            )
