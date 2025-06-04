import os

import pandas as pd
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake._internal import CloneMetrics, DeltaError


def test_basic_shallow_clone_should_create_a_new_table_and_reference_the_same_data(
    tmp_path, sample_table
):
    source_table_path = os.path.join(tmp_path, "source_table")
    write_deltalake(source_table_path, sample_table)
    target_table_path = os.path.join(tmp_path, "shallow_cloned_table")

    source_table = DeltaTable(source_table_path)
    source_data = source_table.to_pandas()
    metrics: CloneMetrics = source_table.clone.shallow_clone(
        target_table_uri=target_table_path, if_not_exists=False, replace=False
    )

    assert metrics.source_num_of_files > 0
    assert metrics.source_table_size > 0
    assert metrics.num_removed_files == 0
    assert metrics.num_copied_files == 0
    assert metrics.removed_files_size == 0
    assert metrics.copied_files_size == 0

    target_table = DeltaTable(target_table_path)
    target_data = target_table.to_pandas()

    assert len(target_data) == len(source_data)
    assert list(target_data.columns) == list(source_data.columns)

    pd.testing.assert_frame_equal(
        source_data.sort_values(by=list(source_data.columns)).reset_index(drop=True),
        target_data.sort_values(by=list(target_data.columns)).reset_index(drop=True),
        check_like=True,
    )

    # For shallow clone, file paths in the target log should be absolute to source data files
    # This assertion needs careful handling of how paths are stored and retrieved.
    # Assuming files() returns paths as stored in log, which for shallow clone are absolute.
    source_data_files_abs = {
        (f"{os.path.abspath(source_table_path)}/{f}") for f in source_table.files()
    }
    target_log_files = set(target_table.files())

    assert (
        target_log_files.issubset(source_data_files_abs)
        or target_log_files == source_data_files_abs
    ), "Shallow clone should reference the same data files from the source"

    source_metadata = source_table.metadata()
    clone_metadata = target_table.metadata()
    assert source_metadata.id != clone_metadata.id
    assert clone_metadata.partition_columns == source_metadata.partition_columns
    assert source_metadata.configuration == clone_metadata.configuration
    assert source_metadata.created_time != clone_metadata.created_time


def test_shallow_clone_if_not_exists(tmp_path, sample_table):
    source_table_path = os.path.join(tmp_path, "source_if_not_exists")
    write_deltalake(source_table_path, sample_table)
    target_table_path = os.path.join(tmp_path, "target_if_not_exists")

    source_table = DeltaTable(source_table_path)

    metrics1 = source_table.clone.shallow_clone(
        target_table_uri=target_table_path, if_not_exists=False, replace=False
    )
    target_table_v1 = DeltaTable(target_table_path)
    version1 = target_table_v1.version()
    files_v1 = set(target_table_v1.files())

    # Attempt second clone with if_not_exists=True
    metrics2 = source_table.clone.shallow_clone(
        target_table_uri=target_table_path, if_not_exists=True, replace=False
    )
    target_table_v2 = DeltaTable(target_table_path)
    version2 = target_table_v2.version()
    files_v2 = set(target_table_v2.files())

    assert version1 == version2, "Version should not change if clone is skipped"
    assert files_v1 == files_v2, "Files should not change if clone is skipped"
    assert (
        metrics2.source_table_size == metrics1.source_table_size
    )  # This is source at time of clone call
    assert (
        metrics2.source_num_of_files == metrics1.source_num_of_files
    )  # This is source at time of clone call
    assert metrics2.num_copied_files == 0  # No files copied because it was skipped
    assert metrics2.num_removed_files == 0  # No files removed


def test_shallow_clone_replace(tmp_path, sample_table):
    source_table_path = os.path.join(tmp_path, "source_replace")
    write_deltalake(source_table_path, sample_table)
    target_table_path = os.path.join(tmp_path, "target_replace")

    source_table = DeltaTable(source_table_path)

    # First clone
    source_table.clone.shallow_clone(target_table_uri=target_table_path)
    target_table_v1_data = DeltaTable(target_table_path).to_pandas()

    # Modify source table
    more_data = pd.DataFrame(
        {"id": [3, 4], "price": [10, 11], "sold": [12, 13], "deleted": [True, False]}
    )
    write_deltalake(source_table_path, more_data, mode="append")  # v1
    source_table.update_incremental()  # Ensure source_table object sees the new version
    source_v1_data = source_table.to_pandas()

    # Clone with replace=True
    metrics_replace = source_table.clone.shallow_clone(
        target_table_uri=target_table_path, if_not_exists=False, replace=True
    )
    target_table_v2 = DeltaTable(target_table_path)
    target_v2_data = target_table_v2.to_pandas()

    pd.testing.assert_frame_equal(
        source_v1_data.sort_values(by=list(source_v1_data.columns)).reset_index(
            drop=True
        ),
        target_v2_data.sort_values(by=list(target_v2_data.columns)).reset_index(
            drop=True
        ),
        check_like=True,
    )
    assert len(target_table_v1_data) != len(target_v2_data), (
        "Target should have been replaced with new data"
    )
    assert metrics_replace.num_copied_files == 0  # Shallow clone
    assert metrics_replace.num_removed_files >= 0


def test_shallow_clone_error_if_exists(tmp_path, sample_table):
    source_table_path = os.path.join(tmp_path, "source_error_if_exists")
    write_deltalake(source_table_path, sample_table)
    target_table_path = os.path.join(tmp_path, "target_error_if_exists")

    source_table = DeltaTable(source_table_path)

    # First clone
    source_table.clone.shallow_clone(
        target_table_uri=target_table_path, if_not_exists=False, replace=False
    )

    # Attempt second clone (default: if_not_exists=False, replace=False)
    with pytest.raises(DeltaError, match="Target table .* already exists"):
        source_table.clone.shallow_clone(
            target_table_uri=target_table_path, if_not_exists=False, replace=False
        )


def test_shallow_clone_mutually_exclusive_flags_should_raise_exception(
    tmp_path, sample_table
):
    source_table_path = os.path.join(tmp_path, "source_exclusive_flags")
    write_deltalake(source_table_path, sample_table)
    target_table_path = os.path.join(tmp_path, "target_exclusive_flags")
    source_table = DeltaTable(source_table_path)

    with pytest.raises(
        ValueError, match="if_not_exists and replace flags cannot both be true"
    ):
        source_table.clone.shallow_clone(
            target_table_uri=target_table_path, if_not_exists=True, replace=True
        )
