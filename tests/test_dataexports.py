import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataexports import PUBLIC_COLUMNS, create_download_files


SOURCE_COLUMNS = [
    source_name
    for _, source_name in PUBLIC_COLUMNS
    if source_name is not None
]


class DataExportsTest(unittest.TestCase):
    def _write_fixture(self, directory: Path, protocol: str) -> Path:
        columns = [
            column
            for column in SOURCE_COLUMNS
            if not (protocol == "udp" and column == "timestamp_response")
        ]
        rows = [
            {
                "ip_request": "192.0.2.1",
                "ip_response": "192.0.2.2",
                "a_record": "192.0.2.3",
                "timestamp_request": "2026-09-20 12:34:56.123",
                "timestamp_response": "2026-09-20 12:34:56.456",
                "response_type": "Forwarder",
                "country_request": "DEU",
                "asn_request": "64500",
                "prefix_request": "192.0.2.0/24",
                "org_request": 'Example; "quoted" GmbH',
                "country_response": "DEU",
                "asn_response": "64501",
                "prefix_response": "192.0.2.0/24",
                "org_response": "Antwort Ü",
                "country_arecord": "DEU",
                "asn_arecord": "64502",
                "prefix_arecord": "192.0.2.0/24",
                "org_arecord": "Backend",
            },
            {column: "" for column in columns},
        ]
        rows = [{column: row.get(column, "") for column in columns} for row in rows]
        csv_path = directory / f"{protocol}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=columns, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)
        return csv_path

    def test_creates_all_formats_with_matching_rows_and_public_schema(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            downloads = root / "downloads"

            for protocol in ("tcp", "udp"):
                source = self._write_fixture(root, protocol)
                publication = create_download_files(
                    str(source), protocol, "2026-09-20", str(downloads)
                )
                self.assertEqual(2, publication["row_count"])

                connection = duckdb.connect()
                try:
                    for output_format, file_info in publication["files"].items():
                        output_path = downloads / file_info["name"]
                        self.assertTrue(output_path.exists(), output_format)
                        if output_format == "parquet":
                            query = f"SELECT * FROM read_parquet('{output_path}')"
                        else:
                            query = f"SELECT * FROM read_csv('{output_path}', header=true)"
                        result = connection.execute(query)
                        self.assertEqual(
                            [name for name, _ in PUBLIC_COLUMNS],
                            [description[0] for description in result.description],
                        )
                        self.assertEqual(2, len(result.fetchall()))
                finally:
                    connection.close()

            with (downloads / "manifest.json").open(encoding="utf-8") as manifest_file:
                manifest = json.load(manifest_file)
            self.assertEqual({"tcp", "udp"}, set(manifest["protocols"]))

    def test_failed_generation_keeps_previous_manifest(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            downloads = root / "downloads"
            source = self._write_fixture(root, "tcp")
            create_download_files(str(source), "tcp", "2026-09-20", str(downloads))
            original_manifest = (downloads / "manifest.json").read_text(encoding="utf-8")

            empty_source = root / "empty.csv"
            with empty_source.open("w", encoding="utf-8", newline="") as csv_file:
                writer = csv.DictWriter(
                    csv_file,
                    fieldnames=SOURCE_COLUMNS,
                    delimiter=";",
                )
                writer.writeheader()

            with self.assertRaises(ValueError):
                create_download_files(
                    str(empty_source), "tcp", "2026-09-27", str(downloads)
                )

            self.assertEqual(
                original_manifest,
                (downloads / "manifest.json").read_text(encoding="utf-8"),
            )
            self.assertFalse((downloads / "odns-tcp-2026-09-27.csv").exists())


if __name__ == "__main__":
    unittest.main()
