"""CSV validation and parsing service."""

import csv
import io
import re
from typing import ClassVar, NamedTuple

from app.models.domain import HospitalRowData
from app.models.schemas import CSVValidationErrorItem


class CSVParseResult(NamedTuple):
    rows: list[HospitalRowData]
    errors: list[CSVValidationErrorItem]
    total_rows: int = 0


class CSVValidationError(Exception):
    """Raised when CSV validation fails before processing."""

    def __init__(self, message: str, errors: list[CSVValidationErrorItem] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.errors = errors or []


class MaxRowsExceededError(CSVValidationError):
    """Raised when CSV row count exceeds maximum allowed limit."""

    pass


class CSVParser:
    """Parses and validates hospital CSV files."""

    REQUIRED_HEADERS: ClassVar[set[str]] = {"name", "address"}
    OPTIONAL_HEADERS: ClassVar[set[str]] = {"phone"}
    MAX_FIELD_LENGTH = 1024
    _PHONE_REGEX = re.compile(r"^[0-9+\-()\s]{7,20}$")
    _CONTROL_CHARS_TABLE: ClassVar[dict[int, None]] = {
        c: None for c in range(32) if c not in (9, 10, 13)
    } | {127: None}

    def __init__(self, max_rows: int = 20) -> None:
        self.max_rows = max_rows

    def _strip_control_chars(self, text: str) -> str:
        """Remove ASCII control characters except newline and tab."""
        return text.translate(self._CONTROL_CHARS_TABLE)

    def parse(self, content_bytes: bytes) -> CSVParseResult:
        """Parse raw CSV bytes into validated hospital rows or return error details."""
        try:
            # Handle utf-8-sig to automatically strip BOM if present
            text = content_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise CSVValidationError(f"Invalid file encoding. Expected UTF-8: {exc}") from exc

        if not text.strip():
            raise CSVValidationError("CSV file is empty.")

        stream = io.StringIO(text)
        reader = csv.reader(stream)

        try:
            raw_headers = next(reader, None)
        except csv.Error as exc:
            raise CSVValidationError(f"Malformed CSV header: {exc}") from exc

        if not raw_headers:
            raise CSVValidationError("CSV header is missing.")

        # Normalize headers: strip whitespace, control chars, and lowercase
        normalized_headers = [self._strip_control_chars(h).strip().lower() for h in raw_headers]

        # Check for empty headers
        if any(h == "" for h in normalized_headers):
            raise CSVValidationError("CSV header contains empty column name.")

        # Check for duplicate headers
        if len(normalized_headers) != len(set(normalized_headers)):
            raise CSVValidationError("CSV header contains duplicate column names.")

        header_map: dict[str, int] = {h: idx for idx, h in enumerate(normalized_headers)}

        missing = self.REQUIRED_HEADERS - set(header_map.keys())
        if missing:
            raise CSVValidationError(
                f"Missing required CSV column(s): {', '.join(sorted(missing))}"
            )

        name_idx = header_map["name"]
        address_idx = header_map["address"]
        phone_idx = header_map.get("phone")

        rows: list[HospitalRowData] = []
        errors: list[CSVValidationErrorItem] = []
        processed_row_numbers: set[int] = set()

        try:
            for row_items in reader:
                current_line_num = reader.line_num

                # Skip blank lines
                if not row_items or all(not item.strip() for item in row_items):
                    continue

                processed_row_numbers.add(current_line_num)
                row_errors: list[CSVValidationErrorItem] = []

                # Extract and sanitize name
                raw_name = (
                    self._strip_control_chars(row_items[name_idx]).strip()
                    if name_idx < len(row_items)
                    else ""
                )
                if not raw_name:
                    row_errors.append(
                        CSVValidationErrorItem(
                            row=current_line_num, field="name", message="Name is required."
                        )
                    )
                elif len(raw_name) > self.MAX_FIELD_LENGTH:
                    row_errors.append(
                        CSVValidationErrorItem(
                            row=current_line_num,
                            field="name",
                            message=f"Name exceeds maximum length of {self.MAX_FIELD_LENGTH} characters.",
                        )
                    )

                # Extract and sanitize address
                raw_address = (
                    self._strip_control_chars(row_items[address_idx]).strip()
                    if address_idx < len(row_items)
                    else ""
                )
                if not raw_address:
                    row_errors.append(
                        CSVValidationErrorItem(
                            row=current_line_num, field="address", message="Address is required."
                        )
                    )
                elif len(raw_address) > self.MAX_FIELD_LENGTH:
                    row_errors.append(
                        CSVValidationErrorItem(
                            row=current_line_num,
                            field="address",
                            message=f"Address exceeds maximum length of {self.MAX_FIELD_LENGTH} characters.",
                        )
                    )

                # Extract and sanitize phone
                phone: str | None = None
                if phone_idx is not None and phone_idx < len(row_items):
                    raw_phone = self._strip_control_chars(row_items[phone_idx]).strip()
                    if raw_phone:
                        if len(raw_phone) > self.MAX_FIELD_LENGTH:
                            row_errors.append(
                                CSVValidationErrorItem(
                                    row=current_line_num,
                                    field="phone",
                                    message=f"Phone exceeds maximum length of {self.MAX_FIELD_LENGTH} characters.",
                                )
                            )
                        elif not self._PHONE_REGEX.match(raw_phone):
                            row_errors.append(
                                CSVValidationErrorItem(
                                    row=current_line_num,
                                    field="phone",
                                    message="Invalid phone format.",
                                )
                            )
                        else:
                            phone = raw_phone

                if row_errors:
                    errors.extend(row_errors)
                else:
                    rows.append(
                        HospitalRowData(
                            row=current_line_num,
                            name=raw_name,
                            address=raw_address,
                            phone=phone,
                        )
                    )
        except csv.Error as exc:
            raise CSVValidationError(f"Malformed CSV content: {exc}") from exc

        if not rows and not errors:
            raise CSVValidationError("CSV contains no hospital records.")

        total_records = len(processed_row_numbers)
        if total_records > self.max_rows:
            raise MaxRowsExceededError(
                f"CSV row count ({total_records}) exceeds maximum allowed limit of {self.max_rows}."
            )

        return CSVParseResult(rows=rows, errors=errors, total_rows=total_records)
