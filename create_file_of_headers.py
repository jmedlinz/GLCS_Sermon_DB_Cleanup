"""
This script processes table and column metadata from a master TSV file and a Microsoft Access database.
It merges this metadata with an existing headers file (if present) and generates a new headers TSV file.
The script ensures consistency between the database, master file, and headers file, while handling enabled/disabled tables.
"""

import csv
import os
from contextlib import contextmanager
from typing import Any, Dict, List, Set, Tuple

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

# Constants
MIN_MASTER_COLUMNS = 3
MIN_HEADER_COLUMNS = 6


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


def verify_files_exist() -> bool:
    """
    Verify that required files exist.

    Returns:
        bool: True if all required files exist, False otherwise
    """
    if not os.path.exists(MASTER_TSV_PATH):
        report_error(f"Error: Master table file not found at '{MASTER_TSV_PATH}'")
        return False

    if not os.path.exists(SOURCE_DB_PATH):
        report_error(f"Error: Database file not found at '{SOURCE_DB_PATH}'")
        return False

    return True


def read_master_tables() -> Tuple[List[Tuple[str, str]], List[str], int, Dict[str, Dict[str, str]]]:
    """
    Read table names from the master TSV file.

    Returns:
        tuple: Contains enabled tables, skipped table names, count of skipped tables, and all tables with enabled status

    Raises:
        Exception: If there's an error reading the master file
    """
    report_subsection("Reading table names from master file")
    tables = []  # Will store (table_id, table_name) tuples
    skipped_tables = 0  # Counter for skipped tables
    skipped_table_names = []  # List to store names of skipped tables
    master_table_enabled = {}  # Dictionary to store enabled status for all tables

    try:
        with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            next(reader)  # Skip header row
            for row in reader:
                if len(row) >= MIN_MASTER_COLUMNS:  # Ensure the row has at least required columns
                    table_id = row[0]  # ID is in the first column
                    table_name = row[1]  # Table name is in the second column
                    enabled = row[2]  # Enabled flag is in the third column

                    # Store all tables in the master_table_enabled dictionary
                    master_table_enabled[table_name] = {"id": table_id, "enabled": enabled}

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

        return tables, skipped_table_names, skipped_tables, master_table_enabled
    except Exception as ex:
        report_error(f"Error reading master table file: {ex}")
        raise


def connect_to_database() -> Tuple[pyodbc.Connection, pyodbc.Cursor]:
    """
    Establish connection to the Access database.

    Returns:
        tuple: Database connection and cursor

    Raises:
        Exception: If connection fails
    """
    report_subsection("Connecting to database")
    report_comment(f"Database file: '{SOURCE_DB_FILE}'")

    try:
        with database_connection() as (conn, cursor):
            report_info("Successfully connected")
            return conn, cursor
    except Exception:
        raise


def fetch_column_info(cursor: pyodbc.Cursor, tables: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    """
    Get column names for each table.

    Args:
        cursor: Database cursor to execute queries
        tables: List of (table_id, table_name) tuples

    Returns:
        list: All headers as (table_id, table_name, column_name) tuples
    """
    report_subsection("Fetching column information for tables")

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
    return all_headers


def read_existing_headers() -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Read existing headers file if it exists.

    Returns:
        dict: Mapping of (table_name, column_name) to column metadata
    """
    existing_headers = {}

    if not os.path.exists(HEADERS_TSV_PATH):
        return existing_headers

    report_subsection("Reading existing headers file for merging")
    try:
        with open(HEADERS_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            _ = next(reader)  # Skip header row

            for row in reader:
                if len(row) >= MIN_HEADER_COLUMNS:
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

    return existing_headers


def merge_headers(
    all_headers: List[Tuple[str, str, str]],
    existing_headers: Dict[Tuple[str, str], Dict[str, str]],
    master_table_enabled: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    """
    Merge the database columns with existing headers.

    Args:
        all_headers: List of (table_id, table_name, column_name) tuples from database
        existing_headers: Dictionary of existing header information
        master_table_enabled: Dictionary of table enabled status from master file

    Returns:
        list: Merged headers with consistent formatting
    """
    # Prepare the merged headers
    merged_headers = []
    processed_keys: Set[Tuple[str, str]] = set()

    # Process tables and columns from the database
    for table_id, table_name, column_name in all_headers:
        key = (table_name, column_name)
        processed_keys.add(key)

        if key in existing_headers:
            # Column exists in both DB and existing file - keep existing data but update enabled status
            existing_data = existing_headers[key]
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

    # Handle entries in existing file that weren't in the database results
    for key, data in existing_headers.items():
        if key in processed_keys:
            continue  # Skip already processed entries

        table_name, column_name = key

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

    return merged_headers


def format_value(value: Any) -> str:
    """
    Format a value for writing to TSV, ensuring consistent handling.

    Args:
        value: The value to format

    Returns:
        str: Formatted value
    """
    if value is None:
        return ""
    elif isinstance(value, str):
        return value.strip()
    else:
        return str(value)


def write_headers_file(
    merged_headers: List[Dict[str, str]],
    tables: List[Tuple[str, str]],
    all_headers: List[Tuple[str, str, str]],
    skipped_tables: int,
    skipped_table_names: List[str],
) -> bool:
    """
    Write the merged headers to a TSV file.

    Args:
        merged_headers: List of merged header dictionaries
        tables: List of (table_id, table_name) tuples
        all_headers: List of all headers as (table_id, table_name, column_name) tuples
        skipped_tables: Number of skipped tables
        skipped_table_names: List of skipped table names

    Returns:
        bool: Success status
    """
    report_subsection("Creating output file")

    # Assign sequential column IDs if needed
    column_id_counter = 1
    for header in merged_headers:
        if not header["column_id"]:
            header["column_id"] = str(column_id_counter)
            column_id_counter += 1

    try:
        with open(HEADERS_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
            writer = csv.writer(tsv_file, delimiter="\t")
            # Write header row
            writer.writerow(["Table_ID", "Table_Name", "Column_ID", "Enabled", "Column_Name", "New_Column_Name"])

            # Write merged data
            for header in merged_headers:
                writer.writerow(
                    [
                        format_value(header["table_id"]),
                        format_value(header["table_name"]),
                        format_value(header["column_id"]),
                        format_value(header["enabled"]),
                        format_value(header["column_name"]),
                        format_value(header["new_column_name"]),
                    ]
                )

        report_info("Successfully wrote headers to TSV")
        report_comment(f"Filename: '{HEADERS_TSV_FILE}'")
        report_comment(f"Processed {len(tables)} tables with {len(all_headers)} columns")
        if skipped_tables > 0:
            report_comment(f"Skipped {skipped_tables} disabled tables:")
            for table_name in sorted(skipped_table_names):
                report_comment(f"   - {table_name}")

        return True
    except Exception as ex:
        report_error(f"Error writing headers file: {ex}")
        return False


def main() -> None:
    """Main function to orchestrate the process."""
    report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)
    report_section("Create Headers File From Access Database")

    try:
        # Verify files exist
        if not verify_files_exist():
            return

        # Read table names from master TSV
        tables, skipped_table_names, skipped_tables, master_table_enabled = read_master_tables()

        # Connect to database and process
        with database_connection() as (conn, cursor):
            # Fetch column information
            all_headers = fetch_column_info(cursor, tables)

            # Read existing headers if present
            existing_headers = read_existing_headers()

            # Merge headers
            merged_headers = merge_headers(all_headers, existing_headers, master_table_enabled)

            # Write headers file
            write_headers_file(merged_headers, tables, all_headers, skipped_tables, skipped_table_names)

    except Exception as ex:
        report_error(f"An unexpected error occurred: {ex}")
        exit(1)


if __name__ == "__main__":
    main()
