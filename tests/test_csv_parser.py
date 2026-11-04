"""Unit tests for CSV parser."""

import pytest

from app.services.csv_parser import CSVParser, CSVValidationError, MaxRowsExceededError


def test_parse_valid_csv() -> None:
    content = b"name,address,phone\nHospital A,123 Main St,555-1234\nHospital B,456 Elm St,\n"
    parser = CSVParser(max_rows=20)
    result = parser.parse(content)

    assert len(result.rows) == 2
    assert len(result.errors) == 0
    assert result.rows[0].name == "Hospital A"
    assert result.rows[0].address == "123 Main St"
    assert result.rows[0].phone == "555-1234"
    assert result.rows[1].name == "Hospital B"
    assert result.rows[1].address == "456 Elm St"
    assert result.rows[1].phone is None


def test_parse_with_bom() -> None:
    bom = b"\xef\xbb\xbf"
    content = bom + b"name,address,phone\nHospital BOM,789 Oak Ave,555-0000\n"
    parser = CSVParser(max_rows=20)
    result = parser.parse(content)

    assert len(result.rows) == 1
    assert result.rows[0].name == "Hospital BOM"


def test_parse_empty_csv() -> None:
    parser = CSVParser(max_rows=20)
    with pytest.raises(CSVValidationError, match="CSV file is empty"):
        parser.parse(b"   \n\n  ")


def test_parse_missing_headers() -> None:
    parser = CSVParser(max_rows=20)
    with pytest.raises(CSVValidationError, match="Missing required CSV column"):
        parser.parse(b"title,location\nHospital A,123 Main St\n")


def test_parse_max_rows_exceeded() -> None:
    parser = CSVParser(max_rows=3)
    content = b"name,address\nH1,Addr1\nH2,Addr2\nH3,Addr3\nH4,Addr4\n"
    with pytest.raises(MaxRowsExceededError, match="exceeds maximum allowed limit"):
        parser.parse(content)


def test_parse_row_level_validation_errors() -> None:
    parser = CSVParser(max_rows=20)
    content = (
        b"name,address,phone\n"
        b",123 Main St,555-1234\n"
        b"General Hospital,,555-5678\n"
        b"Valid Clinic,789 Oak St,555-9999\n"
    )
    result = parser.parse(content)
    assert len(result.rows) == 1
    assert len(result.errors) == 2
    assert result.errors[0].row == 2
    assert result.errors[0].field == "name"
    assert result.errors[1].row == 3
    assert result.errors[1].field == "address"


def test_parse_field_exceeding_max_length() -> None:
    parser = CSVParser(max_rows=20)
    long_name = "A" * 1025
    long_addr = "B" * 1025
    long_phone = "1" * 1025
    content = f"name,address,phone\n{long_name},123 Main St,+1-555-1234\nValid Name,{long_addr},+1-555-1234\nValid Name 2,456 Elm St,{long_phone}\n".encode()
    result = parser.parse(content)

    assert len(result.rows) == 0
    assert len(result.errors) == 3
    assert result.errors[0].field == "name"
    assert "exceeds maximum length" in result.errors[0].message
    assert result.errors[1].field == "address"
    assert "exceeds maximum length" in result.errors[1].message
    assert result.errors[2].field == "phone"
    assert "exceeds maximum length" in result.errors[2].message


def test_parse_invalid_phone_format() -> None:
    parser = CSVParser(max_rows=20)
    content = b"name,address,phone\nGood Hospital,123 Main St,INVALID_PHONE_###\n"
    result = parser.parse(content)

    assert len(result.rows) == 0
    assert len(result.errors) == 1
    assert result.errors[0].field == "phone"
    assert result.errors[0].message == "Invalid phone format."


def test_parse_ascii_control_characters_stripped() -> None:
    parser = CSVParser(max_rows=20)
    # Include \x01, \x07, \x7f in name, address, phone
    content = b"name,address,phone\nHosp\x01ital\x07 Central,123 Main\x1b St,555-123\x7f4\n"
    result = parser.parse(content)

    assert len(result.errors) == 0
    assert len(result.rows) == 1
    assert result.rows[0].name == "Hospital Central"
    assert result.rows[0].address == "123 Main St"
    assert result.rows[0].phone == "555-1234"


def test_parse_duplicate_headers() -> None:
    parser = CSVParser(max_rows=20)
    content = b"name,address,name\nHospital A,123 Main St,Hospital A\n"
    with pytest.raises(CSVValidationError, match="duplicate column names"):
        parser.parse(content)


def test_parse_empty_headers() -> None:
    parser = CSVParser(max_rows=20)
    content = b"name,,address\nHospital A,extra,123 Main St\n"
    with pytest.raises(CSVValidationError, match="empty column name"):
        parser.parse(content)
