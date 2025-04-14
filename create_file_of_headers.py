"""
Column Information Generator Script

This script creates a file that lists all columns from all tables in a Microsoft Access database.
It works in three main steps:
1. Reads the master_table.tsv file to know which tables to process
2. Connects to the Access database to get column information for each table
3. Creates a columns.tsv file with all the column information

The script will preserve any customizations from a previous columns.tsv file if one exists.
It handles tables that are enabled or disabled according to the master_table.tsv file.
"""

import csv  # For reading and writing TSV files
import os  # For checking if files exist and working with file paths
from typing import Dict, List, Set, Tuple  # For type hints to make code clearer

import pyodbc  # Library for connecting to Microsoft Access databases

# Import configuration settings and reporting functions
from config import (
    APP_ENVIRONMENT,
    APP_NAME,
    COMPUTERNAME,
    HEADERS_TSV_FILE,
    HEADERS_TSV_PATH,
    MASTER_TSV_PATH,
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
from utils import check_file_exists, database_connection, format_value

# Constants - minimum number of columns we expect in our files
MIN_MASTER_COLUMNS = 3  # ID, Table Name, Enabled
MIN_HEADER_COLUMNS = 6  # Table ID, Table Name, Column ID, Enabled, Column Name, New Column Name


def verify_files_exist() -> bool:
    """
    Makes sure all required files exist before we start processing.

    Returns:
        bool: True if all files exist, False if any are missing
    """
    # Check if the master table file exists
    if not check_file_exists(MASTER_TSV_PATH, f"Error: Master table file not found at '{MASTER_TSV_PATH}'"):
        return False

    # Check if the database file exists
    if not check_file_exists(SOURCE_DB_PATH, f"Error: Database file not found at '{SOURCE_DB_PATH}'"):
        return False

    # All files exist
    return True


def read_master_tables() -> Tuple[List[Tuple[str, str]], List[str], int, Dict[str, Dict[str, str]]]:
    """
    Reads the master_table.tsv file to get information about all tables.

    The master file tells us:
    - Which tables exist in the database
    - Whether each table is enabled or disabled
    - What ID is assigned to each table

    Returns:
        - A list of enabled tables as (table_id, table_name) pairs
        - A list of table names that were skipped (disabled)
        - The count of skipped tables
        - A dictionary with information about all tables (both enabled and disabled)
    """
    report_subsection("Reading table names from master file")

    tables = []  # Will store (table_id, table_name) pairs for enabled tables
    skipped_tables = 0  # Counter for how many tables we're skipping
    skipped_table_names = []  # Names of tables we're skipping
    master_table_enabled = {}  # Information about all tables (enabled and disabled)

    try:
        # Open and read the master TSV file
        with open(MASTER_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            next(reader)  # Skip the header row

            # Process each row in the file
            for row in reader:
                # Make sure the row has enough columns
                if len(row) >= MIN_MASTER_COLUMNS:
                    table_id = row[0]  # ID is in the first column
                    table_name = row[1]  # Table name is in the second column
                    enabled = row[2]  # Enabled flag is in the third column

                    # Store information about all tables
                    master_table_enabled[table_name] = {"id": table_id, "enabled": enabled}

                    # Only process enabled tables (where enabled = "1")
                    if enabled == "1":
                        tables.append((table_id, table_name))
                    else:
                        # Keep track of disabled tables we're skipping
                        skipped_tables += 1
                        skipped_table_names.append(table_name)

        # Report what we found
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


def fetch_column_info(cursor: pyodbc.Cursor, tables: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    """
    Gets all column names for each table from the database.

    Args:
        cursor: Database cursor for running queries
        tables: List of (table_id, table_name) pairs to process

    Returns:
        A list of (table_id, table_name, column_name) tuples for all columns
    """
    report_subsection("Fetching column information for tables")

    all_headers = []  # Will store all the column information we find

    # Process each enabled table
    for table_id, table_name in tables:
        try:
            # Get all columns for this table
            columns = []
            for column in cursor.columns(table=table_name):
                column_name = column.column_name
                columns.append(column_name)

            # Add each column to our master list with its table information
            for column_name in columns:
                all_headers.append((table_id, table_name, column_name))

            report_info(f"Retrieved {len(columns)} columns from table {table_name}")
        except pyodbc.Error as ex:
            # Report error but continue with other tables
            report_error_continue(f"Error fetching columns for table '{table_name}': {ex}")

    # Report total columns found
    report_info(f"Retrieved {len(all_headers)} total columns from all tables")
    return all_headers


def read_existing_headers() -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Reads an existing columns.tsv file if one exists.

    This lets us preserve customizations from a previous run, such as:
    - Column IDs that were already assigned
    - New column names that were customized
    - Enabled/disabled status for individual columns

    Returns:
        A dictionary mapping (table_name, column_name) to column information
    """
    existing_headers = {}  # Will store information from the existing file

    # Check if the headers file exists
    if not os.path.exists(HEADERS_TSV_PATH):
        return existing_headers  # Return empty dictionary if no file exists

    report_subsection("Reading existing headers file for merging")
    try:
        # Open and read the existing headers file
        with open(HEADERS_TSV_PATH, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            _ = next(reader)  # Skip header row

            # Process each row in the file
            for row in reader:
                # Make sure the row has enough columns
                if len(row) >= MIN_HEADER_COLUMNS:
                    # Extract values from the row
                    table_id, table_name, column_id, enabled, column_name, new_column_name = row

                    # Store information using (table_name, column_name) as the key
                    existing_headers[(table_name, column_name)] = {
                        "table_id": table_id,
                        "column_id": column_id,
                        "enabled": enabled,
                        "new_column_name": new_column_name,
                    }

        report_info(f"Read {len(existing_headers)} columns from existing headers file")
    except Exception as ex:
        # Report error but continue (we'll create a new file)
        report_error_continue(f"Error reading existing headers file: {ex}")
        report_info("Will create a new headers file")

    return existing_headers


def merge_headers(
    all_headers: List[Tuple[str, str, str]],
    existing_headers: Dict[Tuple[str, str], Dict[str, str]],
    master_table_enabled: Dict[str, Dict[str, str]],
) -> List[Dict[str, str]]:
    """
    Combines information from the database and existing headers file.

    This function:
    1. Takes columns we found in the database
    2. Merges them with information from the existing headers file
    3. Updates enabled status based on the master table file

    Args:
        all_headers: List of (table_id, table_name, column_name) from database
        existing_headers: Dictionary of existing header information
        master_table_enabled: Dictionary with enabled status for all tables

    Returns:
        A list of dictionaries with complete information for each column
    """
    # Prepare the merged headers list and track which columns we've processed
    merged_headers = []
    processed_keys: Set[Tuple[str, str]] = set()  # Track which (table, column) pairs we've handled

    # PART 1: Process columns we found in the database
    for table_id, table_name, column_name in all_headers:
        key = (table_name, column_name)
        processed_keys.add(key)  # Mark this column as processed

        # Check if this column exists in the previous headers file
        if key in existing_headers:
            # Column exists in both the database and existing file
            existing_data = existing_headers[key]
            merged_headers.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "column_id": existing_data["column_id"],  # Keep existing ID
                    "enabled": master_table_enabled[table_name]["enabled"],  # Use table's enabled status
                    "column_name": column_name,
                    "new_column_name": existing_data["new_column_name"],  # Keep custom name
                }
            )
        else:
            # This is a new column from the database
            merged_headers.append(
                {
                    "table_id": table_id,
                    "table_name": table_name,
                    "column_id": "",  # Empty ID will be assigned during write
                    "enabled": master_table_enabled[table_name]["enabled"],  # Use table's enabled status
                    "column_name": column_name,
                    "new_column_name": column_name,  # Default to original name
                }
            )

    # PART 2: Handle columns that were in existing file but not found in database
    for key, data in existing_headers.items():
        # Skip columns we already processed
        if key in processed_keys:
            continue

        # Unpack the key
        table_name, column_name = key

        # Check if the table exists in our master table file
        if table_name not in master_table_enabled:
            # Table doesn't exist in master - handle differently based on enabled status
            if data["enabled"] == "1":
                # Column was enabled, but table isn't in master - disable it
                merged_headers.append(
                    {
                        "table_id": data["table_id"],
                        "table_name": table_name,
                        "column_id": data["column_id"],
                        "enabled": "0",  # Disable this column
                        "column_name": column_name,
                        "new_column_name": data["new_column_name"],
                    }
                )
            else:
                # Column was already disabled, keep it as is
                merged_headers.append(
                    {
                        "table_id": data["table_id"],
                        "table_name": table_name,
                        "column_id": data["column_id"],
                        "enabled": data["enabled"],  # Keep existing enabled status
                        "column_name": column_name,
                        "new_column_name": data["new_column_name"],
                    }
                )
        else:
            # Table exists in master but column wasn't in database (could be from disabled table)
            merged_headers.append(
                {
                    "table_id": data["table_id"],
                    "table_name": table_name,
                    "column_id": data["column_id"],
                    "enabled": master_table_enabled[table_name]["enabled"],  # Match master table
                    "column_name": column_name,
                    "new_column_name": data["new_column_name"],
                }
            )

    # Sort by table name and column name to make the output file organized
    merged_headers.sort(key=lambda x: (x["table_name"], x["column_name"]))

    return merged_headers


def write_headers_file(
    merged_headers: List[Dict[str, str]],
    tables: List[Tuple[str, str]],
    all_headers: List[Tuple[str, str, str]],
    skipped_tables: int,
    skipped_table_names: List[str],
) -> bool:
    """
    Writes all the column information to the columns.tsv file.

    Args:
        merged_headers: List of dictionaries with column information
        tables: List of tables we processed
        all_headers: List of all columns we found
        skipped_tables: Number of tables we skipped
        skipped_table_names: Names of tables we skipped

    Returns:
        True if successful, False if an error occurred
    """
    report_subsection("Creating output file")

    # Assign sequential numbers to any columns that don't have an ID
    column_id_counter = 1
    for header in merged_headers:
        if not header["column_id"]:
            header["column_id"] = str(column_id_counter)
            column_id_counter += 1

    try:
        # Open the file for writing (overwrite if it exists)
        with open(HEADERS_TSV_PATH, "w", newline="", encoding="utf-8") as tsv_file:
            writer = csv.writer(tsv_file, delimiter="\t")

            # Write the header row with column names
            writer.writerow(["Table_ID", "Table_Name", "Column_ID", "Enabled", "Column_Name", "New_Column_Name"])

            # Write each column's information
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

        # Report success and statistics
        report_info("Successfully wrote headers to TSV")
        report_comment(f"Filename: '{HEADERS_TSV_FILE}'")
        report_comment(f"Processed {len(tables)} tables with {len(all_headers)} columns")

        # Report information about skipped tables
        if skipped_tables > 0:
            report_comment(f"Skipped {skipped_tables} disabled tables:")
            for table_name in sorted(skipped_table_names):
                report_comment(f"   - {table_name}")

        return True
    except Exception as ex:
        report_error(f"Error writing headers file: {ex}")
        return False


def main() -> None:
    """
    Main function that runs the entire process step by step.

    Steps:
    1. Check if required files exist
    2. Read table information from master file
    3. Connect to the database and get column information
    4. Read existing headers file (if any)
    5. Merge all the information
    6. Write the updated headers file
    """
    # Display header with information about the program and environment
    report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)
    report_section("Create Headers File From Access Database")

    try:
        # Step 1: Verify that required files exist
        if not verify_files_exist():
            return  # Exit if files are missing

        # Step 2: Read information about tables from the master file
        tables, skipped_table_names, skipped_tables, master_table_enabled = read_master_tables()

        # Step 3: Connect to database and get column information
        with database_connection() as (conn, cursor):
            # Step 3a: Get column information for all enabled tables
            all_headers = fetch_column_info(cursor, tables)

            # Step 4: Read the existing headers file (if any)
            existing_headers = read_existing_headers()

            # Step 5: Merge information from database and existing file
            merged_headers = merge_headers(all_headers, existing_headers, master_table_enabled)

            # Step 6: Write the merged information to the headers file
            write_headers_file(merged_headers, tables, all_headers, skipped_tables, skipped_table_names)

    except Exception as ex:
        # Handle any unexpected errors
        report_error(f"An unexpected error occurred: {ex}")
        exit(1)  # Exit with error code


# This runs the main function when the script is executed directly
if __name__ == "__main__":
    main()
