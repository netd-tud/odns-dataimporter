import csv
import psycopg
from psycopg import sql
import fieldtypers as ft
import zipFileUtils as zu
import logging


# Database connection configuration
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": "5432"
}

# Table name
TABLE_NAME = "odns.dns_entries"
BATCHLIMIT = 500
# Mapping of CSV headers to table columns for both types of CSVs
CSV_COLUMNS_MAP = {
    "tcp": [
        "ip_request", "ip_response", "a_record", "timestamp_request", 
        "timestamp_response", "response_type", "country_request", 
        "asn_request", "prefix_request", "org_request", "country_response", 
        "asn_response", "prefix_response", "org_response", 
        "country_arecord", "asn_arecord", "prefix_arecord", "org_arecord"
    ],
    "udp": [
        "ip_request", "ip_response", "a_record", "timestamp_request", 
        "response_type", "country_request", "asn_request", "prefix_request", 
        "org_request", "country_response", "asn_response", "prefix_response", 
        "org_response", "country_arecord", "asn_arecord", "prefix_arecord", 
        "org_arecord"
    ]
}

DATABASE_COLUMNS = [
    "protocol","ip_request", "ip_response", "a_record", "timestamp_request", 
    "timestamp_response", "response_type", "country_request", 
    "asn_request", "prefix_request", "org_request", "country_response", 
    "asn_response", "prefix_response", "org_response", 
    "country_arecord", "asn_arecord", "prefix_arecord", "org_arecord"
]
#Test
ARCHIVE_DIRECTORY = r"C:\MyFiles\Projects\ODNS\data"
TEMP_OUTPUT_DIRECTORY = r"C:\MyFiles\Projects\ODNS\data"
PROCESSED_DIRECTORY = r"C:\MyFiles\Projects\ODNS\data\processed"
# Logging vars
LOGGING_FILE = r"C:\MyFiles\Projects\ODNS\data\processed\odnsdataimporter_logs.log"

#Live
#ARCHIVE_DIRECTORY = r"/nfs-dns-data/"
#TEMP_OUTPUT_DIRECTORY = r"/home/backend/odns_temp_data/"
#PROCESSED_DIRECTORY = r"/nfs-dns-data/processed/"
#LOGGING_FILE = r"/home/backend/odns_dataimporter/logs/logs.log"

ARCHIVE_EXTENTION = "csv.gz"
TCP_PREFIX = "tcp"
UDP_PREFIX = "udp"



Logger = logging.getLogger(__name__)
logging.basicConfig(filename=LOGGING_FILE, encoding='utf-8', level=logging.INFO , format='%(asctime)s %(levelname)s %(message)s',datefmt='%Y-%m-%d %I:%M:%S')


# Insert data into the database
def insert_data(cursor, table_name, data, columns):
    insert_query = sql.SQL(
        "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
    ).format(
        table=sql.SQL(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
        placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in columns)
    )
    cursor.execute(insert_query, data)

# Read CSV and insert data
def process_csv(file_path, file_type, connection,scan_date):
    columns = CSV_COLUMNS_MAP[file_type]
    columns.append('protocol')
    columns.append("scan_date")
    with open(file_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=";")
        #print(csv_reader.fieldnames)
        #next(csv_reader)
        with connection.cursor() as cursor:
            bulkCount = 0
            for row in csv_reader:
                # Ensure all columns exist in row, filling missing ones with None
                
                row['protocol'] = file_type
                row['scan_date'] = scan_date
                data = []
                for col in columns:
                    if  col in ft.fieldtypers.keys():
                        data.append(ft.fieldtypers[col](row.get(col, None)))
                    else:
                        # keeping none none
                        data.append(None if row.get(col, None) == "" else row.get(col, None))
                        
                insert_data(cursor, TABLE_NAME, data, columns)
                bulkCount = bulkCount + 1
                #To-Do implement a batch count before commit
                if bulkCount >= BATCHLIMIT:
                    connection.commit()
                    # test
                    return
        connection.commit()

def main():
    # Example file paths
    tcp_csv_path = "" #r"C:\MyFiles\Projects\ODNS\data\tcp_dataframe_complete.csv"
    udp_csv_path = "" #r"C:\MyFiles\Projects\ODNS\data\udp_dataframe_complete.csv"
    
    try:
        # Connect to PostgreSQL
        with psycopg.connect(**DB_CONFIG) as conn:
            #print("Connected to the database.")
            Logger.info("Connected to the database")
            #t = conn.cursor()
            #t.execute("select * from odns.dns_entries")
            #print(t.fetchall())
            
            # Process both CSV files
            #print("Processing TCP CSV...")
            Logger.info("Started processing TCP dns data")
            tcp_csv_path,archive_tcp_csv_path= zu.unzip_recent_file_with_prefix(directory=ARCHIVE_DIRECTORY,prefix=TCP_PREFIX,extention=ARCHIVE_EXTENTION,outputDir=TEMP_OUTPUT_DIRECTORY)
            
            if tcp_csv_path:
                scan_tcp_date = zu.extract_file_date_from_name(archive_tcp_csv_path)
                process_csv(tcp_csv_path, "tcp", conn,scan_tcp_date)
                zu.delete_file(tcp_csv_path)
                if archive_tcp_csv_path:
                    zu.move_processed_file(archive_tcp_csv_path,PROCESSED_DIRECTORY)
                    #print("Cleaned after processing files for TCP")
                    Logger.info("Cleaned after processing files for TCP")
            
            #print("Processing UDP CSV...")
            Logger.info("Started processing UDP dns data")
            udp_csv_path,archive_udp_csv_path= zu.unzip_recent_file_with_prefix(directory=ARCHIVE_DIRECTORY,prefix=UDP_PREFIX,extention=ARCHIVE_EXTENTION,outputDir=TEMP_OUTPUT_DIRECTORY)
            
            if udp_csv_path:
                scan_udp_date = zu.extract_file_date_from_name(archive_udp_csv_path)
                process_csv(udp_csv_path, "udp", conn,scan_udp_date)
                zu.delete_file(udp_csv_path)
                if archive_udp_csv_path:
                    zu.move_processed_file(archive_udp_csv_path,PROCESSED_DIRECTORY)
                    #print("Cleaned after processing files for UDP")
                    Logger.info("Cleaned after processing files for UDP")
            
            #print("Data insertion completed successfully.")
            Logger.info("Data insertion completed successfully")
    except Exception as e:
        Logger.error(f"Error occured: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
