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

    # Dictionary to store enabled status for all tables from master file
    master_table_enabled = {}
    with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
        reader = csv.reader(tsv_file, delimiter="\t")
        next(reader)  # Skip header row
        for row in reader:
            if len(row) >= 3:
                table_id, table_name, enabled = row[0], row[1], row[2]
                master_table_enabled[table_name] = {"id": table_id, "enabled": enabled}

    # Create a dictionary of all tables and columns from the database query
    db_headers = {(table_name, column_name): table_id for table_id, table_name, column_name in all_headers}

    # Check if headers file exists and read it if it does
    existing_headers = {}
    if os.path.exists(HEADERS_TSV_PATH):
        report_subsection("Reading existing headers file for merging")
        try:
            with open(HEADERS_TSV_PATH, encoding="utf-8") as tsv_file:
                reader = csv.reader(tsv_file, delimiter="\t")
                header_row = next(reader)  # Skip header row

                for row in reader:
                    if len(row) >= 6:
                        table_id, table_name, column_id, enabled, column_name, new_column_name = row
                        existing_headers[(table_name, column_name)] = {
                            "table_id": table_id,
                            "column_id": column_id,
                            "enabled": enabled,
                            "new_column_name": new_column_name,
                        }

            report_info(f"Read {len(existing_headers)} columns from existing headers file")
        except Exception as ex:
            report_error_continue(f"Error reading existing headers file: {ex}")
            report_info("Will create a new headers file")
            existing_headers = {}

    # Prepare the merged headers
    merged_headers = []

    # Process tables and columns from the database
    for table_id, table_name, column_name in all_headers:
        if (table_name, column_name) in existing_headers:
            # Column exists in both DB and existing file - keep existing data but update enabled status
            existing_data = existing_headers[(table_name, column_name)]
            merged_headers.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "column_id": existing_data["column_id"],
                    "enabled": master_table_enabled[table_name]["enabled"],  # Update enabled to match master
                    "column_name": column_name,
                    "new_column_name": existing_data["new_column_name"],
                }
            )
        else:
            # New column from DB - add with enabled status from master
            merged_headers.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "column_id": "",  # Will be assigned during write
                    "enabled": master_table_enabled[table_name]["enabled"],
                    "column_name": column_name,
                    "new_column_name": column_name,  # Default new name to original name
                }
            )

        # Mark as processed
        if (table_name, column_name) in existing_headers:
            existing_headers[(table_name, column_name)]["processed"] = True

    # Handle entries in existing file that weren't in the database results
    for (table_name, column_name), data in existing_headers.items():
        if data.get("processed"):
            continue  # Skip already processed entries

        if table_name not in master_table_enabled:
            # Table in headers but not in master - if enabled, set to disabled
            if data["enabled"] == "1":
                merged_headers.append(
                    {
                        "table_id": data["table_id"],
                        "table_name": table_name,
                        "column_id": data["column_id"],
                        "enabled": "0",  # Set to disabled
                        "column_name": column_name,
                        "new_column_name": data["new_column_name"],
                    }
                )
            else:
                # Already disabled, keep as is
                merged_headers.append(
                    {
                        "table_id": data["table_id"],
                        "table_name": table_name,
                        "column_id": data["column_id"],
                        "enabled": data["enabled"],
                        "column_name": column_name,
                        "new_column_name": data["new_column_name"],
                    }
                )
        else:
            # Table in both, but column not retrieved from DB (possibly disabled table in master)
            merged_headers.append(
                {
                    "table_id": data["table_id"],
                    "table_name": table_name,
                    "column_id": data["column_id"],
                    "enabled": master_table_enabled[table_name]["enabled"],  # Set to match master
                    "column_name": column_name,
                    "new_column_name": data["new_column_name"],
                }
            )

    # Sort by table name and column name
    merged_headers.sort(key=lambda x: (x["table_name"], x["column_name"]))

    # Assign sequential column IDs if needed
    column_id_counter = 1
    for header in merged_headers:
        if not header["column_id"]:
            header["column_id"] = str(column_id_counter)
            column_id_counter += 1

    # Write headers to TSV file
    report_subsection("Creating output file")
    with open(HEADERS_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
        writer = csv.writer(tsv_file, delimiter="\t")
        # Write header row
        writer.writerow(["Table_ID", "Table_Name", "Column_ID", "Enabled", "Column_Name", "New_Column_Name"])

        # Write merged data
        for header in merged_headers:
            table_name_trimmed = header["table_name"].strip()
            column_name_trimmed = header["column_name"].strip()

            writer.writerow(
                [
                    header["table_id"].strip() if isinstance(header["table_id"], str) else header["table_id"],
                    table_name_trimmed,
                    header["column_id"],
                    header["enabled"],
                    column_name_trimmed,
                    (
                        header["new_column_name"].strip()
                        if isinstance(header["new_column_name"], str)
                        else header["new_column_name"]
                    ),
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
