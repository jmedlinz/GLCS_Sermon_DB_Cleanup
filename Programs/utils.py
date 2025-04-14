"""
Utility functions shared across multiple scripts in the GLCS Sermon DB Cleanup project.

This module provides common functionality like:
- Database connections
- File existence checking
- TSV file handling helpers
"""

import os
from contextlib import contextmanager
from typing import Any, List, Optional

import pyodbc

# Import configuration and reporting
from .config import SOURCE_DB_PATH
from .report import report_error, report_error_continue, report_info


@contextmanager
def database_connection():
    """
    Creates a safe connection to the Microsoft Access database.

    This is a context manager that allows us to use a "with" statement for database connections.
    It ensures the database connection is properly closed even if errors occur.

    Usage:
        with database_connection() as (connection, cursor):
            # do something with the connection and cursor
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


def check_file_exists(file_path: str, error_message: Optional[str] = None) -> bool:
    """
    Makes sure a required file exists before processing.

    Args:
        file_path: The path to the file to check
        error_message: Custom error message to display if file is missing

    Returns:
        True if the file exists, False if it doesn't
    """
    if not os.path.exists(file_path):
        if error_message:
            report_error(error_message)
        else:
            report_error(f"Error: Required file not found at '{file_path}'")
        return False
    return True


def format_value(value: Any) -> str:
    """
    Formats a value consistently for writing to the TSV file.

    Args:
        value: Any value that needs to be written

    Returns:
        A properly formatted string
    """
    if value is None:
        return ""  # Convert None to empty string
    elif isinstance(value, str):
        return value.strip()  # Remove leading/trailing spaces
    else:
        return str(value)  # Convert anything else to string


def fetch_table_names(cursor: pyodbc.Cursor) -> List[str]:
    """
    Gets a list of all user tables from the Access database.

    Args:
        cursor: Database cursor for executing queries

    Returns:
        A list of table names found in the database
    """
    report_info("Fetching table names from database")
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
        raise
