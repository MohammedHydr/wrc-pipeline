"""Dagster wiring: partitioned run config, schedule targeting, and config
validation. Pure — no Dagster daemon, no network, no DB."""

from datetime import datetime

import dagster as dg
import pytest

from orchestration.wrc_dagster.definitions import (
    WrcPipelineConfig,
    _validate_config,
    wrc_monthly_config,
    wrc_monthly_incremental,
    wrc_pipeline,
)


def _op_config(partition_key: str) -> dict:
    run_config = wrc_monthly_config.get_run_config_for_partition_key(partition_key)
    return run_config["ops"]["scrape_landing_zone"]["config"]


def test_monthly_partition_maps_to_inclusive_calendar_month():
    cfg = _op_config("2024-01-01")
    assert cfg["start_date"] == "2024-01-01"
    assert cfg["end_date"] == "2024-01-31"  # Dagster's exclusive end -> inclusive
    assert cfg["partition"] == "monthly"
    assert cfg["bodies"] == ""  # empty = all configured bodies


def test_monthly_partition_handles_leap_february():
    assert _op_config("2024-02-01")["end_date"] == "2024-02-29"


def test_job_is_monthly_partitioned():
    keys = wrc_pipeline.partitions_def.get_partition_keys()
    assert keys[0] == "2024-01-01"
    assert all(key.endswith("-01") for key in keys[:3])


def test_schedule_targets_previous_month_partition():
    ctx = dg.build_schedule_context(scheduled_execution_time=datetime(2026, 7, 2, 6, 0))
    request = wrc_monthly_incremental(ctx)
    assert request.partition_key == "2026-06-01"


def test_validate_config_rejects_reversed_date_range():
    config = WrcPipelineConfig(start_date="2024-02-01", end_date="2024-01-01")
    with pytest.raises(dg.Failure, match="must not be after"):
        _validate_config(config)


def test_validate_config_rejects_unknown_partition_size():
    config = WrcPipelineConfig(partition="hourly")
    with pytest.raises(dg.Failure, match="Unsupported partition"):
        _validate_config(config)
