"""
This script connects to a Microsoft Access database and retrieves the names of all user tables (excluding system tables).
It then writes these table names to a TSV (Tab-Separated Values) file with additional columns for "ID", "Enabled", "Event", and "Series".
"""

import csv
import os

import pyodbc

from config import (
    APP_ENVIRONMENT,
    APP_NAME,
    COMPUTERNAME,
    MASTER_TSV_FILE,
    MASTER_TSV_PATH,
    SOURCE_DB_FILE,
    SOURCE_DB_PATH,
    USERNAME,
)
from report import (
    report_comment,
    report_error,
    report_error_continue,
    report_header,
    report_info,
    report_section,
    report_subsection,
)

report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)

report_section("Create Master Table From Access Database")

# Check if the database file exists
if not os.path.exists(SOURCE_DB_PATH):
    report_error(f"Error: Database file not found at '{SOURCE_DB_PATH}'")
    exit()

# Data structure to hold existing file entries
existing_entries = {}
next_id = 1

# Check if the output file already exists
if os.path.exists(MASTER_TSV_PATH):
    report_info(f"Master TSV file already exists at '{MASTER_TSV_PATH}'. Will merge with database information.")
    try:
        with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            headers = next(reader)  # Skip header row

            for row in reader:
                if len(row) >= 5:  # Ensure the row has all needed columns
                    table_id = int(row[0]) if row[0].isdigit() else 0
                    table_name = row[1].strip()
                    enabled = row[2]
                    event = row[3]
                    series = row[4]

                    existing_entries[table_name] = {
                        "id": table_id,
                        "enabled": enabled,
                        "event": event,
                        "series": series,
                    }

                    # Keep track of the highest ID for new entries
                    next_id = max(next_id, table_id + 1)

        report_info(f"Read {len(existing_entries)} table entries from existing master file")
    except Exception as ex:
        report_error(f"Error reading existing master file: {ex}")
        exit(1)

# Construct the connection string
# You might need to adjust the driver name based on your installed version
# Common drivers:
# 'Microsoft Access Driver (*.mdb, *.accdb)'
# 'Microsoft Access Driver (*.mdb)'
conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" r"DBQ=" + SOURCE_DB_PATH + ";"

table_names = []
conn = None  # Initialize conn to None
cursor = None  # Initialize cursor to None

report_subsection("Connecting to database")
report_comment(f"Database file: '{SOURCE_DB_FILE}'")

try:
    # Establish the database connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    report_info("Successfully connected")

    report_subsection("Fetching table names")

    # Fetch table names (excluding system tables starting with 'MSys')
    for row in cursor.tables(tableType="TABLE"):
        table_name = row.table_name
        if not table_name.startswith("MSys"):
            table_names.append(table_name)

    report_info(f"Retrieved {len(table_names)} user tables from database")

    # Track changes for reporting
    added_tables = []
    removed_tables = []
    retained_tables = []

    # Process merging logic
    merged_entries = {}

    # 1. Process tables from database
    for table in sorted(table_names):
        table = table.strip()
        if table in existing_entries:
            # Table exists in both - retain file info
            merged_entries[table] = existing_entries[table]
            retained_tables.append(table)
        else:
            # Table in DB but not in file - add with defaults
            merged_entries[table] = {"id": next_id, "enabled": "1", "event": "0", "series": "0"}
            next_id += 1
            added_tables.append(table)

    # 2. Identify tables in file but not in DB
    for table in existing_entries:
        if table not in table_names:
            removed_tables.append(table)

    # Write merged data to TSV file
    report_subsection("Creating merged output file")
    with open(MASTER_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
        writer = csv.writer(tsv_file, delimiter="\t")
        writer.writerow(["ID", "Table Name", "Enabled", "Event", "Series"])

        for table in sorted(merged_entries.keys()):
            entry = merged_entries[table]
            writer.writerow([entry["id"], table, entry["enabled"], entry["event"], entry["series"]])

    # Report merge statistics
    report_info(f"Successfully wrote {len(merged_entries)} table entries to master TSV file")
    report_comment(f"Filename: '{MASTER_TSV_FILE}'")

    if added_tables:
        report_comment(f"Added {len(added_tables)} new tables:")
        for table in added_tables:
            report_comment(f"   - {table}")

    if removed_tables:
        report_comment(f"Removed {len(removed_tables)} tables not found in database:")
        for table in removed_tables:
            report_comment(f"   - {table}")

    report_comment(f"Retained {len(retained_tables)} existing tables")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    report_error("Error connecting to database or fetching tables.")
    report_error_continue(f"SQLSTATE: {sqlstate}")
    report_error_continue(f"Message: {ex}")
    if "IM002" in sqlstate:
        report_error_continue("This error often means the ODBC driver is not installed or not found.")
        report_error_continue(
            "Ensure the Microsoft Access Database Engine Redistributable is installed (32-bit or 64-bit matching your Python)."
        )


finally:
    # Ensure the connection is closed
    if cursor:
        cursor.close()
        # report_comment("Cursor closed.")
    if conn:
        conn.close()
        # report_comment("Database connection closed.")
