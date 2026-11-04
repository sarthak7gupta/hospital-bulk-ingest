"""Generate comprehensive sample CSV datasets for good and bad test scenarios."""

from pathlib import Path


def generate_samples(target_dir: Path) -> None:
    """Generate categorized sample CSV datasets for automated and manual verification."""
    target_dir.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------
    # GOOD CASES
    # -------------------------------------------------------------

    # 1. Single valid hospital
    (target_dir / "good_single.csv").write_text(
        "name,address,phone\nCity General Hospital,100 Medical Plaza,555-0100\n",
        encoding="utf-8",
    )

    # 2. Standard 5 hospitals (mixed phone values)
    (target_dir / "good_standard_5.csv").write_text(
        "name,address,phone\n"
        "St. Jude Children's Research Hospital,262 Danny Thomas Place Memphis TN,888-226-4343\n"
        "Mayo Clinic Hospital,1216 2nd St SW Rochester MN,507-284-2511\n"
        "Cleveland Clinic,9500 Euclid Ave Cleveland OH,800-223-2273\n"
        "Johns Hopkins Hospital,1800 Orleans St Baltimore MD,\n"
        "Massachusetts General Hospital,55 Fruit St Boston MA,617-726-2000\n",
        encoding="utf-8",
    )

    # 3. Maximum boundary limit (exactly 20 hospitals)
    rows_20 = ["name,address,phone"] + [
        f"Metro Health Clinic {i:02d},{i:02d} Parkway Blvd,555-{i:04d}" for i in range(1, 21)
    ]
    (target_dir / "good_max_limit_20.csv").write_text(
        "\n".join(rows_20) + "\n",
        encoding="utf-8",
    )

    # 4. Quoted fields with internal commas and spaces
    (target_dir / "good_quotes_and_commas.csv").write_text(
        "name,address,phone\n"
        '"Memorial Hospital, East Wing","100 North Blvd, Suite 400, Chicago IL","555-9001"\n'
        '"Children\'s Health Center, West Campus","200 South St, Floor 3, Dallas TX",""\n',
        encoding="utf-8",
    )

    # 5. UTF-8 file with Byte Order Mark (BOM)
    bom_content = (
        b"\xef\xbb\xbfname,address,phone\nBOM Test Hospital,500 Evergreen Terrace,555-1212\n"
    )
    (target_dir / "good_utf8_bom.csv").write_bytes(bom_content)

    # -------------------------------------------------------------
    # BAD CASES
    # -------------------------------------------------------------

    # 1. Empty 0-byte file
    (target_dir / "bad_empty_file.csv").write_bytes(b"")

    # 2. Only whitespace and empty newlines
    (target_dir / "bad_all_whitespace.csv").write_text(
        "   \n\n\t  \n  \n",
        encoding="utf-8",
    )

    # 3. Oversized batch (25 rows > 20 limit)
    rows_25 = ["name,address,phone"] + [
        f"Overflow Hospital {i:02d},{i:02d} Overflow Blvd,555-{i:04d}" for i in range(1, 26)
    ]
    (target_dir / "bad_oversized_25.csv").write_text(
        "\n".join(rows_25) + "\n",
        encoding="utf-8",
    )

    # 4. Missing required name
    (target_dir / "bad_missing_name.csv").write_text(
        "name,address,phone\n,123 Maple St,555-1111\n   ,456 Pine St,555-2222\n",
        encoding="utf-8",
    )

    # 5. Missing required address
    (target_dir / "bad_missing_address.csv").write_text(
        "name,address,phone\nSunrise Hospital,,555-3333\nSunset Hospital,   ,555-4444\n",
        encoding="utf-8",
    )

    # 6. Missing required header columns
    (target_dir / "bad_missing_required_headers.csv").write_text(
        "hospital_title,location,telephone\nAlpha Hospital,100 Alpha St,555-0001\n",
        encoding="utf-8",
    )

    # 7. Mixed valid and invalid rows
    (target_dir / "bad_mixed_failures.csv").write_text(
        "name,address,phone\n"
        "Valid Hospital 1,100 Valid St,555-1000\n"
        ",Missing Name St,555-2000\n"
        "Valid Hospital 2,200 Valid St,555-3000\n"
        "Missing Address Hospital,,555-4000\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    out_dir = Path(__file__).resolve().parent.parent / "sample_data"
    generate_samples(out_dir)
    print(f"Sample datasets successfully written to {out_dir}")
