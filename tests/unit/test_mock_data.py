from pathlib import Path

import pytest

from quant_core.ingestion.mock_data import MockDataConfig, generate_mock_data, month_bounds, validate_yyyymm


def test_validate_yyyymm_rejects_bad_format():
    with pytest.raises(ValueError):
        validate_yyyymm("2026-01")


def test_month_bounds_for_february():
    start_dt, end_dt = month_bounds("202602")
    assert start_dt.isoformat() == "2026-02-01"
    assert end_dt.isoformat() == "2026-02-28"


def test_generate_mock_data_creates_month_folders(tmp_path):
    config = MockDataConfig(base_dir=Path(tmp_path), target_yyyymm="202604")
    summary = generate_mock_data(config)
    assert summary["target_yyyymm"] == "202604"
    assert (tmp_path / "transactions" / "202604").exists()
    assert (tmp_path / "dimensions" / "202604").exists()

