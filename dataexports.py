import json
import os
import re
import uuid
from pathlib import Path

import duckdb


DOWNLOAD_FORMATS = {
    "csv": ("csv", "text/csv"),
    "csv.zst": ("csv.zst", "application/zstd"),
    "parquet": ("parquet", "application/vnd.apache.parquet"),
}

PUBLIC_COLUMNS = [
    ("protocol", None),
    ("queried_ip", "ip_request"),
    ("replying_ip", "ip_response"),
    ("backend_resolver", "a_record"),
    ("timestamp_request", "timestamp_request"),
    ("timestamp_response", "timestamp_response"),
    ("resolver_type", "response_type"),
    ("queried_ip_country", "country_request"),
    ("replying_ip_country", "country_response"),
    ("queried_ip_asn", "asn_request"),
    ("replying_ip_asn", "asn_response"),
    ("queried_ip_prefix", "prefix_request"),
    ("replying_ip_prefix", "prefix_response"),
    ("queried_ip_org", "org_request"),
    ("replying_ip_org", "org_response"),
    ("backend_resolver_country", "country_arecord"),
    ("backend_resolver_asn", "asn_arecord"),
    ("backend_resolver_prefix", "prefix_arecord"),
    ("backend_resolver_org", "org_arecord"),
    ("scan_date", None),
]

BIGINT_COLUMNS = {"asn_request", "asn_response", "asn_arecord"}
TIMESTAMP_COLUMNS = {"timestamp_request", "timestamp_response"}


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _select_query(csv_path: str, protocol: str, scan_date: str) -> str:
    expressions = []
    for public_name, source_name in PUBLIC_COLUMNS:
        if public_name == "protocol":
            value = _sql_string(protocol)
        elif public_name == "scan_date":
            value = _sql_string(scan_date)
        elif source_name == "timestamp_response" and protocol == "udp":
            value = "NULL::TIMESTAMP"
        elif source_name in BIGINT_COLUMNS:
            value = f'TRY_CAST(NULLIF("{source_name}", \'\') AS BIGINT)'
        elif source_name in TIMESTAMP_COLUMNS:
            value = f'TRY_CAST(NULLIF("{source_name}", \'\') AS TIMESTAMP)'
        else:
            value = f'"{source_name}"'
        if public_name == "scan_date":
            value = f"CAST({_sql_string(scan_date)} AS DATE)"
        expressions.append(f'{value} AS "{public_name}"')

    return (
        "SELECT "
        + ", ".join(expressions)
        + " FROM read_csv("
        + _sql_string(os.path.abspath(csv_path))
        + ", delim = ';', header = true, all_varchar = true, nullstr = '')"
    )


def _copy_query(select_query: str, output_path: Path, output_format: str) -> str:
    destination = _sql_string(str(output_path.resolve()))
    if output_format == "csv":
        options = "FORMAT CSV, HEADER, DELIMITER ','"
    elif output_format == "csv.zst":
        options = "FORMAT CSV, HEADER, DELIMITER ',', COMPRESSION ZSTD"
    else:
        options = "FORMAT PARQUET, COMPRESSION ZSTD"
    return f"COPY ({select_query}) TO {destination} ({options})"


def _load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.exists():
        return {"version": 1, "protocols": {}}
    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)
    if manifest.get("version") != 1 or not isinstance(manifest.get("protocols"), dict):
        raise ValueError("Unsupported download manifest")
    return manifest


def _write_manifest(manifest_path: Path, manifest: dict) -> None:
    temporary_path = manifest_path.with_name(
        f".{manifest_path.name}.{uuid.uuid4().hex}.tmp"
    )
    try:
        with temporary_path.open("w", encoding="utf-8") as manifest_file:
            json.dump(manifest, manifest_file, indent=2, sort_keys=True)
            manifest_file.write("\n")
            manifest_file.flush()
            os.fsync(manifest_file.fileno())
        os.replace(temporary_path, manifest_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _cleanup_old_exports(download_directory: Path, protocol: str) -> None:
    filename_pattern = re.compile(
        rf"^odns-{re.escape(protocol)}-(\d{{4}}-\d{{2}}-\d{{2}})\.(csv|csv\.zst|parquet)$"
    )
    dated_files = []
    for path in download_directory.iterdir():
        match = filename_pattern.match(path.name)
        if match:
            dated_files.append((match.group(1), path))

    retained_dates = sorted({date for date, _ in dated_files}, reverse=True)[:2]
    for date, path in dated_files:
        if date not in retained_dates:
            path.unlink(missing_ok=True)


def create_download_files(
    csv_path: str, protocol: str, scan_date: str, download_directory: str
) -> dict:
    if protocol not in ("tcp", "udp"):
        raise ValueError(f"Unsupported protocol: {protocol}")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", scan_date or ""):
        raise ValueError(f"Invalid scan date: {scan_date}")

    output_directory = Path(download_directory)
    output_directory.mkdir(parents=True, exist_ok=True)
    staging_directory = output_directory / f".staging-{protocol}-{uuid.uuid4().hex}"
    staging_directory.mkdir()

    select_query = _select_query(csv_path, protocol, scan_date)
    created_files = {}
    expected_count = None
    connection = duckdb.connect()
    try:
        for output_format, (extension, content_type) in DOWNLOAD_FORMATS.items():
            filename = f"odns-{protocol}-{scan_date}.{extension}"
            staging_path = staging_directory / filename
            result = connection.execute(
                _copy_query(select_query, staging_path, output_format)
            ).fetchone()
            row_count = result[0] if result else None
            if row_count is None or row_count <= 0:
                raise ValueError(f"Generated {filename} without any rows")
            if expected_count is None:
                expected_count = row_count
            elif row_count != expected_count:
                raise ValueError(
                    f"Generated {filename} with {row_count} rows, expected {expected_count}"
                )
            created_files[output_format] = {
                "name": filename,
                "size": staging_path.stat().st_size,
                "content_type": content_type,
            }

        for file_info in created_files.values():
            os.replace(
                staging_directory / file_info["name"],
                output_directory / file_info["name"],
            )

        manifest_path = output_directory / "manifest.json"
        manifest = _load_manifest(manifest_path)
        manifest["protocols"][protocol] = {
            "scan_date": scan_date,
            "row_count": expected_count,
            "files": created_files,
        }
        _write_manifest(manifest_path, manifest)
        _cleanup_old_exports(output_directory, protocol)
        return manifest["protocols"][protocol]
    finally:
        connection.close()
        for path in staging_directory.glob("*"):
            path.unlink(missing_ok=True)
        staging_directory.rmdir()
