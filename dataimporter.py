import csv
import argparse
import configparser as cp
import psycopg
from psycopg import sql
import fieldtypers as ft
import zipFileUtils as zu
import logging
import os
import sys
import time

# Database connection configuration
config = cp.ConfigParser()
config.read("config.ini")
DB_CONFIG = config["db-connection-params"]
# Table name
TABLE_NAME = config["db-table-names"]["odnstable"]
BATCHLIMIT = 200000
# Mapping of CSV headers to table columns for both types of CSVs
CSV_COLUMNS_MAP = {
    "tcp": [
        "ip_request",
        "ip_response",
        "a_record",
        "timestamp_request",
        "timestamp_response",
        "response_type",
        "country_request",
        "asn_request",
        "prefix_request",
        "org_request",
        "country_response",
        "asn_response",
        "prefix_response",
        "org_response",
        "country_arecord",
        "asn_arecord",
        "prefix_arecord",
        "org_arecord",
    ],
    "udp": [
        "ip_request",
        "ip_response",
        "a_record",
        "timestamp_request",
        "response_type",
        "country_request",
        "asn_request",
        "prefix_request",
        "org_request",
        "country_response",
        "asn_response",
        "prefix_response",
        "org_response",
        "country_arecord",
        "asn_arecord",
        "prefix_arecord",
        "org_arecord",
    ],
}

DATABASE_COLUMNS = [
    "protocol",
    "ip_request",
    "ip_response",
    "a_record",
    "timestamp_request",
    "timestamp_response",
    "response_type",
    "country_request",
    "asn_request",
    "prefix_request",
    "org_request",
    "country_response",
    "asn_response",
    "prefix_response",
    "org_response",
    "country_arecord",
    "asn_arecord",
    "prefix_arecord",
    "org_arecord",
]

IS_TESTING = False

# Live
ARCHIVE_DIRECTORY = r"/data/"
TEMP_OUTPUT_DIRECTORY = r"/tmp/"
PROCESSED_DIRECTORY = r"/data/processed/"
LOGGING_FILE = r"/logs/logs.log"

ARCHIVE_EXTENTION = "csv.gz"
TCP_PREFIX = "tcp"
UDP_PREFIX = "udp"


Logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=LOGGING_FILE,
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S",
)


# Insert data into the database
def insert_data(cursor, table_name, data, columns):
    insert_query = sql.SQL(
        "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
    ).format(
        table=sql.SQL(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    cursor.executemany(insert_query, data)


# Read CSV and insert data
def process_csv(file_path, file_type, connection, scan_date):
    columns = CSV_COLUMNS_MAP[file_type]
    columns.append("protocol")
    columns.append("scan_date")
    with open(file_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=";")
        # print(csv_reader.fieldnames)
        # next(csv_reader)
        records = []
        with connection.cursor() as cursor:
            bulkCount = 0
            start_time = time.time()  # Start timer
            for row in csv_reader:
                # Ensure all columns exist in row, filling missing ones with None
                row["protocol"] = file_type
                row["scan_date"] = scan_date
                data = []
                for col in columns:
                    if col in ft.fieldtypers.keys():
                        data.append(ft.fieldtypers[col](row.get(col, None)))
                    else:
                        # keeping none none
                        data.append(
                            None if row.get(col, None) == "" else row.get(col, None)
                        )
                records.append(data)
                # insert_data(cursor, TABLE_NAME, data, columns)
                bulkCount = bulkCount + 1
                # To-Do implement a batch count before commit
                if bulkCount >= BATCHLIMIT:
                    insert_data(cursor, TABLE_NAME, records, columns)
                    connection.commit()
                    end_time = time.time()  # End timer
                    execution_time = end_time - start_time
                    Logger.debug(
                        f"Bulk count: {bulkCount} Execution Time: {execution_time:.6f} seconds"
                    )
                    start_time = time.time()  # Start timer
                    bulkCount = 0
                    records.clear()
                    # test
                    if IS_TESTING:
                        return
        if records:
            bulkCount = len(records)
            start_time = time.time()  # Start timer
            with connection.cursor() as cursor:
                insert_data(cursor, TABLE_NAME, records, columns)
            connection.commit()
            end_time = time.time()  # End timer
            execution_time = end_time - start_time
            Logger.debug(
                f"Bulk count: {bulkCount} Execution Time: {execution_time:.6f} seconds"
            )


def main():
    parser = argparse.ArgumentParser(description="Data Importer Health Check")
    parser.add_argument(
        "--check-health",
        action="store_true",
        help="Check health of Postgres and shared drive",
    )
    args = parser.parse_args()

    if args.check_health:
        print("[*] Health check...")
        pg_status = check_postgres()
        drive_status = check_shared_drive()
        if pg_status and drive_status:
            print("[*] System healthy.")
            Logger.info("System health check successfull")
            sys.exit(0)
        else:
            if not pg_status:
                print("[*] No postgres connection.")
                Logger.error("No postgres connection")
            if not drive_status:
                print("[*] Failed to access data drive.")
                Logger.error("No access to shared drive")
            sys.exit(1)
    # Example file paths
    tcp_csv_path = ""  # r"C:\MyFiles\Projects\ODNS\data\tcp_dataframe_complete.csv"
    udp_csv_path = ""  # r"C:\MyFiles\Projects\ODNS\data\udp_dataframe_complete.csv"

    tcp_csv_path = ""
    udp_csv_path = ""

    print("[*] Processing data.")
    try:
        # Connect to PostgreSQL
        with psycopg.connect(**DB_CONFIG) as conn:
            # print("Connected to the database.")
            # print("Connected to the database.")
            Logger.info("Connected to the database")
            # t = conn.cursor()
            # t.execute("select * from odns.dns_entries")
            # print(t.fetchall())

            # t = conn.cursor()
            # t.execute("select * from odns.dns_entries")
            # print(t.fetchall())

            # Process both CSV files
            # print("Processing TCP CSV...")
            print("[*] Processing TCP CSV...")
            Logger.info("Started processing TCP dns data")
            tcp_csv_path, archive_tcp_csv_path = zu.unzip_recent_file_with_prefix(
                directory=ARCHIVE_DIRECTORY,
                prefix=TCP_PREFIX,
                extention=ARCHIVE_EXTENTION,
                outputDir=TEMP_OUTPUT_DIRECTORY,
            )

            if tcp_csv_path:
                scan_tcp_date = zu.extract_file_date_from_name(archive_tcp_csv_path)
                process_csv(tcp_csv_path, "tcp", conn, scan_tcp_date)
                zu.delete_file(tcp_csv_path)
                if archive_tcp_csv_path:
                    zu.move_processed_file(archive_tcp_csv_path, PROCESSED_DIRECTORY)
                    # print("Cleaned after processing files for TCP")
                    Logger.info("Cleaned after processing files for TCP")

            # print("Processing UDP CSV...")

            print("[*] Processing UDP CSV...")
            Logger.info("Started processing UDP dns data")
            udp_csv_path, archive_udp_csv_path = zu.unzip_recent_file_with_prefix(
                directory=ARCHIVE_DIRECTORY,
                prefix=UDP_PREFIX,
                extention=ARCHIVE_EXTENTION,
                outputDir=TEMP_OUTPUT_DIRECTORY,
            )

            if udp_csv_path:
                scan_udp_date = zu.extract_file_date_from_name(archive_udp_csv_path)
                process_csv(udp_csv_path, "udp", conn, scan_udp_date)
                zu.delete_file(udp_csv_path)
                if archive_udp_csv_path:
                    zu.move_processed_file(archive_udp_csv_path, PROCESSED_DIRECTORY)
                    # print("Cleaned after processing files for UDP")
                    Logger.info("Cleaned after processing files for UDP")

            # print("Data insertion completed successfully.")

            print("[*] Data insertion completed successfully.")
            Logger.info("Data insertion completed successfully")
    except Exception as e:
        Logger.error(f"Error occured: {e}")
        print(f"Error: {e}")


def check_postgres():
    try:
        conn = psycopg.connect(**DB_CONFIG)
        conn.close()
        Logger.info("Postgres connection successful")
        return True
    except Exception as e:
        Logger.error(f"Postgres connection failed: {e}")
        return False


def check_shared_drive():
    if os.path.exists("/data") and os.access("/data", os.R_OK):
        Logger.info("Shared drive is accessible")
        return True
    else:
        Logger.error("Shared drive is not accessible")
        return False


if __name__ == "__main__":
    main()
