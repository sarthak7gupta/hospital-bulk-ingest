"""Tests verifying all generated sample CSV datasets against the validation engine."""

from pathlib import Path

import pytest

from app.services.csv_parser import CSVParser, CSVValidationError, MaxRowsExceededError

SAMPLES_DIR = Path(__file__).resolve().parent.parent / "sample_data"


@pytest.mark.parametrize(
    ("filename", "expected_count"),
    [
        ("good_single.csv", 1),
        ("good_standard_5.csv", 5),
        ("good_max_limit_20.csv", 20),
        ("good_quotes_and_commas.csv", 2),
        ("good_utf8_bom.csv", 1),
    ],
)
def test_good_sample_csvs(filename: str, expected_count: int) -> None:
    path = SAMPLES_DIR / filename
    content = path.read_bytes()
    parser = CSVParser(max_rows=20)
    result = parser.parse(content)

    assert len(result.rows) == expected_count
    assert len(result.errors) == 0


def test_bad_empty_file() -> None:
    content = (SAMPLES_DIR / "bad_empty_file.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    with pytest.raises(CSVValidationError, match="CSV file is empty"):
        parser.parse(content)


def test_bad_all_whitespace() -> None:
    content = (SAMPLES_DIR / "bad_all_whitespace.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    with pytest.raises(CSVValidationError, match="CSV file is empty"):
        parser.parse(content)


def test_bad_oversized_25() -> None:
    content = (SAMPLES_DIR / "bad_oversized_25.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    with pytest.raises(MaxRowsExceededError, match="exceeds maximum allowed limit of 20"):
        parser.parse(content)


def test_bad_missing_name() -> None:
    content = (SAMPLES_DIR / "bad_missing_name.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    result = parser.parse(content)
    assert len(result.rows) == 0
    assert len(result.errors) == 2
    assert all(e.field == "name" for e in result.errors)


def test_bad_missing_address() -> None:
    content = (SAMPLES_DIR / "bad_missing_address.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    result = parser.parse(content)
    assert len(result.rows) == 0
    assert len(result.errors) == 2
    assert all(e.field == "address" for e in result.errors)


def test_bad_missing_required_headers() -> None:
    content = (SAMPLES_DIR / "bad_missing_required_headers.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    with pytest.raises(CSVValidationError, match="Missing required CSV column"):
        parser.parse(content)


def test_bad_mixed_failures() -> None:
    content = (SAMPLES_DIR / "bad_mixed_failures.csv").read_bytes()
    parser = CSVParser(max_rows=20)
    result = parser.parse(content)
    assert len(result.rows) == 2
    assert len(result.errors) == 2
    assert result.errors[0].field == "name"
    assert result.errors[1].field == "address"
