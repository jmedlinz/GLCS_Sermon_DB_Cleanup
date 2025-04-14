"""
This script reads existing table data from a master TSV file and connects to a Microsoft Access database to retrieve user table names.
It merges the existing data with the database table names, identifying added, removed, and retained tables.
The merged data is then written back to the master TSV file, including columns for "ID", "Enabled", "Event", and "Series".
"""

import csv
import os
from contextlib import contextmanager
from typing import Any, Dict, List, Tuple

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

# Constants
MIN_EXPECTED_COLUMNS = 5


@contextmanager
def database_connection():
    """Context manager for database connections."""
    conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" r"DBQ=" + SOURCE_DB_PATH + ";"
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        yield conn, cursor
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        report_error("Error connecting to database.")
        report_error_continue(f"SQLSTATE: {sqlstate}")
        report_error_continue(f"Message: {ex}")
        if "IM002" in sqlstate:
            report_error_continue("This error often means the ODBC driver is not installed or not found.")
            report_error_continue(
                "Ensure the Microsoft Access Database Engine Redistributable is installed (32-bit or 64-bit matching your Python)."
            )
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def read_existing_data() -> Tuple[Dict[str, Dict[str, Any]], int]:
    """
    Read existing data from the master TSV file.

    Returns:
        Tuple containing:
            - Dictionary mapping table names to their properties
            - Next available ID for new entries
    """
    existing_entries = {}
    next_id = 1

    if not os.path.exists(MASTER_TSV_PATH):
        report_info(f"No existing master TSV file found at '{MASTER_TSV_PATH}'.")
        return existing_entries, next_id

    report_info(f"Master TSV file already exists at '{MASTER_TSV_PATH}'. Will merge with database information.")
    try:
        with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            _ = next(reader)  # Skip header row

            for row in reader:
                if len(row) >= MIN_EXPECTED_COLUMNS:  # Ensure the row has all needed columns
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
        raise

    return existing_entries, next_id


def connect_to_database() -> Tuple[pyodbc.Connection, pyodbc.Cursor]:
    """
    Establish connection to the Access database.

    Returns:
        Tuple containing the database connection and cursor

    Raises:
        SystemExit: If the database connection fails
    """
    report_subsection("Connecting to database")
    report_comment(f"Database file: '{SOURCE_DB_FILE}'")

    if not os.path.exists(SOURCE_DB_PATH):
        report_error(f"Error: Database file not found at '{SOURCE_DB_PATH}'")
        exit(1)

    try:
        with database_connection() as (conn, cursor):
            report_info("Successfully connected")
            return conn, cursor
    except Exception:
        exit(1)


def fetch_table_names(cursor: pyodbc.Cursor) -> List[str]:
    """
    Fetch table names from the database.

    Args:
        cursor: Database cursor to execute queries

    Returns:
        List of table names from the database

    Raises:
        SystemExit: If fetching tables fails
    """
    report_subsection("Fetching table names")
    table_names = []

    try:
        # Fetch table names (excluding system tables starting with 'MSys')
        for row in cursor.tables(tableType="TABLE"):
            table_name = row.table_name
            if not table_name.startswith("MSys"):
                table_names.append(table_name)

        report_info(f"Retrieved {len(table_names)} user tables from database")
        return table_names
    except pyodbc.Error as ex:
        report_error(f"Error fetching tables: {ex}")
        exit(1)


def merge_data(
    existing_entries: Dict[str, Dict[str, Any]], table_names: List[str], next_id: int
) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[str], List[str]]:
    """
    Merge existing entries with database tables.

    Args:
        existing_entries: Dictionary of existing table entries from TSV
        table_names: List of table names from the database
        next_id: Next available ID to assign to new entries

    Returns:
        Tuple containing:
            - Dictionary of merged entries
            - List of added table names
            - List of removed table names
            - List of retained table names
    """
    # Track changes for reporting
    added_tables = []
    removed_tables = []
    retained_tables = []

    try:
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

        return merged_entries, added_tables, removed_tables, retained_tables
    except Exception as ex:
        report_error(f"Error merging data: {ex}")
        raise


def write_output(
    merged_entries: Dict[str, Dict[str, Any]],
    added_tables: List[str],
    removed_tables: List[str],
    retained_tables: List[str],
) -> bool:
    """
    Write the merged data to the output TSV file.

    Args:
        merged_entries: Dictionary of merged table entries
        added_tables: List of newly added table names
        removed_tables: List of removed table names
        retained_tables: List of retained table names

    Returns:
        Boolean indicating success
    """
    report_subsection("Creating merged output file")
    try:
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
        return True
    except Exception as ex:
        report_error(f"Error writing output file: {ex}")
        return False


def main() -> None:
    """Main function to orchestrate the process."""
    report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)
    report_section("Create Master Table From Access Database")

    try:
        # Read existing data
        existing_entries, next_id = read_existing_data()

        # Connect to the database
        with database_connection() as (conn, cursor):
            # Fetch table names
            table_names = fetch_table_names(cursor)

            # Merge data
            merged_entries, added_tables, removed_tables, retained_tables = merge_data(
                existing_entries, table_names, next_id
            )

            # Write output
            write_output(merged_entries, added_tables, removed_tables, retained_tables)

    except Exception as ex:
        report_error(f"An unexpected error occurred: {ex}")
        exit(1)


if __name__ == "__main__":
    main()
