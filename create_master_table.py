"""
This script compares tables in a Microsoft Access database with a TSV file tracking those tables.
It updates the TSV file by:
1. Reading existing data from the TSV file (if it exists)
2. Connecting to the Access database to get all user tables
3. Identifying which tables are new, removed, or unchanged
4. Writing all this information back to the TSV file

The TSV file stores important information about each table including:
- ID: A unique number for each table
- Enabled: Whether the table is active (1) or not (0)
- Event: Special flag for event tables (0 or 1)
- Series: Special flag for series tables (0 or 1)
"""

import csv  # For reading/writing TSV files
import os   # For file path operations
from contextlib import contextmanager  # For safer database connections
from typing import Any, Dict, List, Tuple  # Type hints to make code clearer

import pyodbc  # Library for connecting to Microsoft Access

# Import configuration settings and reporting functions
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
MIN_EXPECTED_COLUMNS = 5  # Minimum number of columns we expect in the TSV file


@contextmanager
def database_connection():
    """
    Creates a safe way to connect to the database and automatically close it when done.

    This is a context manager that allows us to use a "with" statement for the database connection.
    It makes sure the database connection is properly closed even if errors occur.
    """
    # Create the connection string for Microsoft Access
    conn_str = r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" r"DBQ=" + SOURCE_DB_PATH + ";"
    conn = None  # Initialize connection variable
    cursor = None  # Initialize cursor variable

    try:
        # Try to connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        yield conn, cursor  # Return the connection and cursor to the caller
    except pyodbc.Error as ex:
        # Handle database connection errors
        sqlstate = ex.args[0]
        report_error("Error connecting to database.")
        report_error_continue(f"SQLSTATE: {sqlstate}")
        report_error_continue(f"Message: {ex}")

        # Provide helpful message for a common error
        if "IM002" in sqlstate:
            report_error_continue("This error often means the ODBC driver is not installed or not found.")
            report_error_continue(
                "Ensure the Microsoft Access Database Engine Redistributable is installed (32-bit or 64-bit matching your Python)."
            )
        raise  # Re-raise the error after logging it
    finally:
        # Always try to close cursor and connection if they were opened
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def read_existing_data() -> Tuple[Dict[str, Dict[str, Any]], int]:
    """
    Reads the current TSV file (if it exists) to get existing table information.

    Returns:
        - A dictionary where:
          * Keys are table names
          * Values are dictionaries with 'id', 'enabled', 'event', 'series' information
        - The next available ID number to use for new tables
    """
    existing_entries = {}  # Dictionary to store existing table information
    next_id = 1  # Start IDs at 1 if no existing file

    # Check if the TSV file already exists
    if not os.path.exists(MASTER_TSV_PATH):
        report_info(f"No existing master TSV file found at '{MASTER_TSV_PATH}'.")
        return existing_entries, next_id

    # If file exists, try to read it
    report_info(f"Master TSV file already exists at '{MASTER_TSV_PATH}'. Will merge with database information.")
    try:
        with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            _ = next(reader)  # Skip the header row (column titles)

            for row in reader:
                # Make sure the row has enough columns
                if len(row) >= MIN_EXPECTED_COLUMNS:
                    # Extract values from each column
                    table_id = int(row[0]) if row[0].isdigit() else 0
                    table_name = row[1].strip()
                    enabled = row[2]
                    event = row[3]
                    series = row[4]

                    # Store the information in our dictionary
                    existing_entries[table_name] = {
                        "id": table_id,
                        "enabled": enabled,
                        "event": event,
                        "series": series,
                    }

                    # Keep track of the highest ID used so we can assign new IDs later
                    next_id = max(next_id, table_id + 1)

        report_info(f"Read {len(existing_entries)} table entries from existing master file")
    except Exception as ex:
        report_error(f"Error reading existing master file: {ex}")
        raise

    return existing_entries, next_id


def fetch_table_names(cursor: pyodbc.Cursor) -> List[str]:
    """
    Gets a list of all user tables from the Access database.

    Args:
        cursor: Database cursor for executing queries

    Returns:
        A list of table names found in the database
    """
    report_subsection("Fetching table names")
    table_names = []  # List to store table names

    try:
        # Get all tables from the database
        # We skip system tables (those starting with 'MSys')
        for row in cursor.tables(tableType="TABLE"):
            table_name = row.table_name
            if not table_name.startswith("MSys"):
                table_names.append(table_name)

        report_info(f"Retrieved {len(table_names)} user tables from database")
        return table_names
    except pyodbc.Error as ex:
        report_error(f"Error fetching tables: {ex}")
        exit(1)  # Exit the program if we can't get table names


def merge_data(
    existing_entries: Dict[str, Dict[str, Any]], table_names: List[str], next_id: int
) -> Tuple[Dict[str, Dict[str, Any]], List[str], List[str], List[str]]:
    """
    Combines existing TSV data with the tables found in the database.

    This function:
    1. Keeps all tables that exist in the database
    2. Identifies new tables that aren't in the TSV yet
    3. Identifies tables in the TSV that no longer exist in the database

    Args:
        existing_entries: Dictionary of existing table information from TSV
        table_names: List of table names found in the database
        next_id: Next available ID number for new tables

    Returns:
        - Dictionary with all tables to keep and their information
        - List of newly added table names
        - List of removed table names
        - List of unchanged table names
    """
    # Lists to track what happened to each table
    added_tables = []     # Tables in DB but not in TSV
    removed_tables = []   # Tables in TSV but not in DB
    retained_tables = []  # Tables in both places

    try:
        merged_entries = {}  # Will hold all tables we want to keep

        # Process each table found in the database
        for table in sorted(table_names):
            table = table.strip()  # Remove any extra spaces

            # Check if this table was already in our TSV
            if table in existing_entries:
                # Keep the existing information for this table
                merged_entries[table] = existing_entries[table]
                retained_tables.append(table)
            else:
                # This is a new table - create default values for it
                merged_entries[table] = {
                    "id": next_id,     # Assign the next available ID
                    "enabled": "1",    # Enable by default
                    "event": "0",      # Not an event by default
                    "series": "0"      # Not a series by default
                }
                next_id += 1  # Increment the ID counter
                added_tables.append(table)

        # Find tables that were in the TSV but not in the database
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
    Writes all the table information back to the TSV file.

    Args:
        merged_entries: Dictionary with all tables and their information
        added_tables: List of newly added tables (for reporting)
        removed_tables: List of removed tables (for reporting)
        retained_tables: List of unchanged tables (for reporting)

    Returns:
        True if successful, False if an error occurred
    """
    report_subsection("Creating merged output file")
    try:
        # Open the file for writing (this will create or overwrite the file)
        with open(MASTER_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
            writer = csv.writer(tsv_file, delimiter="\t")

            # Write the header row with column names
            writer.writerow(["ID", "Table Name", "Enabled", "Event", "Series"])

            # Write each table's information as a row
            for table in sorted(merged_entries.keys()):
                entry = merged_entries[table]
                writer.writerow([entry["id"], table, entry["enabled"], entry["event"], entry["series"]])

        # Print statistics about what changed
        report_info(f"Successfully wrote {len(merged_entries)} table entries to master TSV file")
        report_comment(f"Filename: '{MASTER_TSV_FILE}'")

        # Report new tables that were added
        if added_tables:
            report_comment(f"Added {len(added_tables)} new tables:")
            for table in added_tables:
                report_comment(f"   - {table}")

        # Report tables that were removed
        if removed_tables:
            report_comment(f"Removed {len(removed_tables)} tables not found in database:")
            for table in removed_tables:
                report_comment(f"   - {table}")

        # Report how many tables stayed the same
        report_comment(f"Retained {len(retained_tables)} existing tables")
        return True
    except Exception as ex:
        report_error(f"Error writing output file: {ex}")
        return False


def main() -> None:
    """
    Main function that runs the entire process in sequence.

    Steps:
    1. Display program information header
    2. Read existing TSV data (if any)
    3. Connect to the Access database
    4. Get table names from the database
    5. Merge existing data with database tables
    6. Write everything back to the TSV file
    """
    # Display program header with environmental information
    report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)
    report_section("Create Master Table From Access Database")

    try:
        # Step 1: Read existing data from TSV file (if it exists)
        existing_entries, next_id = read_existing_data()

        # Step 2: Connect to the database and process the data
        with database_connection() as (conn, cursor):
            # Step 3: Get all user tables from the database
            table_names = fetch_table_names(cursor)

            # Step 4: Merge existing data with database tables
            merged_entries, added_tables, removed_tables, retained_tables = merge_data(
                existing_entries, table_names, next_id
            )

            # Step 5: Write everything back to the TSV file
            write_output(merged_entries, added_tables, removed_tables, retained_tables)

    except Exception as ex:
        # Handle any unexpected errors
        report_error(f"An unexpected error occurred: {ex}")
        exit(1)  # Exit with error code


# This checks if the script is being run directly (not imported)
if __name__ == "__main__":
    main()
