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

# Check if the output file already exists
if os.path.exists(MASTER_TSV_PATH):
    report_error(f"Error: Output file '{MASTER_TSV_PATH}' already exists. Remove it or use a different filename.")
    exit(1)

# Check if the database file exists
if not os.path.exists(SOURCE_DB_PATH):
    report_error(f"Error: Database file not found at '{SOURCE_DB_PATH}'")
    exit()

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

    report_info(f"Retrieved {len(table_names)} user tables")

    # Write table names to TSV file with additional "ID", "Enabled", "Event", and "Series" columns
    report_subsection("Creating output file")
    with open(MASTER_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
        writer = csv.writer(tsv_file, delimiter="\t")  # Use tab as the delimiter
        # Strip spaces from header row
        writer.writerow(["ID".strip(), "Table Name".strip(), "Enabled".strip(), "Event".strip(), "Series".strip()])

        for idx, table in enumerate(sorted(table_names), start=1):  # Sort names alphabetically and add ID
            writer.writerow(
                [idx, table.strip(), 1, 0, 0]
            )  # Strip spaces from table name and set default values

    report_info("Successfully wrote table names to TSV")
    report_comment(f"Filename: '{MASTER_TSV_FILE}'")

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
