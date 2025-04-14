"""
This script reads table names from the master TSV file, connects to a Microsoft Access database,
retrieves the column headers for each table, and writes these headers to a TSV file.
"""

import csv
import os

import pyodbc

from config import (
    APP_ENVIRONMENT,
    APP_NAME,
    COMPUTERNAME,
    HEADERS_TSV_FILE,
    HEADERS_TSV_PATH,
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

report_section("Create Headers File From Access Database")

# Check if the master table file exists
if not os.path.exists(MASTER_TSV_PATH):
    report_error(f"Error: Master table file not found at '{MASTER_TSV_PATH}'")
    exit(1)

# Check if the database file exists
if not os.path.exists(SOURCE_DB_PATH):
    report_error(f"Error: Database file not found at '{SOURCE_DB_PATH}'")
    exit(1)

# Read table names from the master TSV file
report_subsection("Reading table names from master file")
tables = []  # Will store (table_id, table_name) tuples
skipped_tables = 0  # Counter for skipped tables
skipped_table_names = []  # List to store names of skipped tables
try:
    with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
        reader = csv.reader(tsv_file, delimiter="\t")
        next(reader)  # Skip header row
        for row in reader:
            if len(row) >= 3:  # Ensure the row has at least 3 columns (including Enabled)
                table_id = row[0]  # ID is in the first column
                table_name = row[1]  # Table name is in the second column
                enabled = row[2]  # Enabled flag is in the third column

                # Only include tables with Enabled = 1
                if enabled == "1":
                    tables.append((table_id, table_name))
                else:
                    skipped_tables += 1
                    skipped_table_names.append(table_name)

    report_info(f"Read {len(tables)} enabled table names from master file")
    if skipped_tables > 0:
        report_info(f"Skipped {skipped_tables} disabled tables")
        report_comment("Skipped tables:")
        for table_name in sorted(skipped_table_names):
            report_comment(f"   - {table_name}")
except Exception as ex:
    report_error(f"Error reading master table file: {ex}")
    exit(1)

# Construct the connection string
conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" r"DBQ=" + SOURCE_DB_PATH + ";"

conn = None
cursor = None

report_subsection("Connecting to database")
report_comment(f"Database file: '{SOURCE_DB_FILE}'")

try:
    # Establish the database connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    report_info("Successfully connected")

    report_subsection("Fetching column information for tables")

    # Get column names for each table and store them
    all_headers = []
    for table_id, table_name in tables:
        try:
            columns = []
            for column in cursor.columns(table=table_name):
                column_name = column.column_name
                columns.append(column_name)

            for column_name in columns:
                all_headers.append((table_id, table_name, column_name))

            report_info(f"Retrieved {len(columns)} columns from table {table_name}")
        except pyodbc.Error as ex:
            report_error_continue(f"Error fetching columns for table '{table_name}': {ex}")

    report_info(f"Retrieved {len(all_headers)} total columns from all tables")

    # Write headers to TSV file
    report_subsection("Creating output file")
    with open(HEADERS_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
        writer = csv.writer(tsv_file, delimiter="\t")
        # Update header row with new column order (already using underscores)
        writer.writerow(["Table_ID", "Table_Name", "Column_ID", "Enabled", "Column_Name", "New_Column_Name"])

        for idx, (table_id, table_name, column_name) in enumerate(
            sorted(all_headers, key=lambda x: (x[1], x[2])), start=1
        ):
            # Only trim leading/trailing spaces, don't replace spaces with underscores
            table_name_trimmed = table_name.strip()
            column_name_trimmed = column_name.strip()

            # Write row with new column order
            writer.writerow(
                [
                    table_id.strip() if isinstance(table_id, str) else table_id,
                    table_name_trimmed,
                    idx,
                    1,  # Enabled column defaulted to 1
                    column_name_trimmed,
                    column_name_trimmed,  # New_Column_Name defaulted to Column_Name
                ]
            )

    report_info("Successfully wrote headers to TSV")
    report_comment(f"Filename: '{HEADERS_TSV_FILE}'")
    report_comment(f"Processed {len(tables)} tables with {len(all_headers)} columns")
    if skipped_tables > 0:
        report_comment(f"Skipped {skipped_tables} disabled tables:")
        for table_name in sorted(skipped_table_names):
            report_comment(f"   - {table_name}")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    report_error("Error connecting to database or fetching columns.")
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
    if conn:
        conn.close()
