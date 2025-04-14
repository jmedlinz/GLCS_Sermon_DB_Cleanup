"""
Column Statistics Analyzer

This script reads the columns.tsv file and generates statistics about the columns:
1. How many times each column name appears across all tables
2. Which tables contain which columns
3. Which tables are missing important columns like "Date", "Title", and "Text"

This helps identify inconsistencies in the database structure and find patterns
in how columns are used across different tables.
"""

import csv  # For reading TSV files
import sys  # For system operations like exit codes
from collections import Counter  # A special dictionary that counts things automatically
from dataclasses import dataclass  # For creating simple data container classes
from typing import Dict, List, Set  # For type hints to make code clearer

# Import configuration settings and reporting functions
from config import (
    APP_ENVIRONMENT,
    APP_NAME,
    COMPUTERNAME,
    HEADERS_TSV_PATH,
    USERNAME,
)
from report import (
    report_comment,
    report_error,
    report_header,
    report_info,
    report_section,
    report_subsection,
)
from utils import check_file_exists

# ----- Settings and Constants -----

# Minimum number of columns we expect in each row of the TSV file
MIN_COLUMN_COUNT = 6

# For columns that appear only a few times, we'll show which tables have them
LOW_FREQUENCY_THRESHOLD = 5

# How wide to make the column name in the report display
COLUMN_NAME_DISPLAY_WIDTH = 50

# How wide to make the count column in the report display
COUNT_DISPLAY_WIDTH = 10

# List of column names that every table should have
ESSENTIAL_COLUMNS = ["Date", "Title", "Text"]


# ----- Data Container Class -----


@dataclass
class HeadersAnalysis:
    """
    A container to hold the analysis results.

    A dataclass is a special type of class that is designed to store data
    without requiring a lot of code. Python automatically creates the __init__
    method based on the fields we define.

    Fields:
        column_counter: Counts how many times each column name appears
        column_tables: Maps each column name to the set of tables it appears in
        table_columns: Maps each table name to the set of columns it contains
        total_columns: The total number of columns processed
    """

    column_counter: Counter  # Counts occurrences of each column name
    column_tables: Dict[str, Set[str]]  # Column name -> set of tables with that column
    table_columns: Dict[str, Set[str]]  # Table name -> set of columns in that table
    total_columns: int  # Total number of columns across all tables


# ----- Helper Functions -----


def read_headers_file(file_path: str = HEADERS_TSV_PATH) -> HeadersAnalysis:
    """
    Reads the columns.tsv file and analyzes all the column data.

    This function:
    1. Opens and reads the TSV file line by line
    2. Counts how often each column name appears
    3. Tracks which tables have which columns
    4. Tracks which columns appear in which tables

    Args:
        file_path: Path to the headers/columns file (default is from config)

    Returns:
        A HeadersAnalysis object with all the statistics and relationships

    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's a problem reading the file
        ValueError: If the file format isn't what we expect
    """
    report_subsection("Reading headers file")

    # Create a Counter object to count column occurrences
    # A Counter is a special dictionary that increments counts automatically
    column_counter = Counter()

    # Keep track of the total number of columns we process
    total_columns = 0

    # Dictionary to track which tables each column appears in
    # Key: column name, Value: set of table names
    column_tables = {}

    # Dictionary to track which columns each table has
    # Key: table name, Value: set of column names
    table_columns = {}

    try:
        # Open and read the TSV file
        with open(file_path, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            next(reader)  # Skip the header row

            # Process each data row (starting from row 2)
            for row_num, row in enumerate(reader, start=2):
                # Make sure the row has enough columns
                if len(row) < MIN_COLUMN_COUNT:
                    report_error(f"Row {row_num} has fewer than {MIN_COLUMN_COUNT} columns. Skipping.")
                    continue

                # Extract the data we need from this row
                table_name = row[1]  # Table_Name is in column 2
                column_name = row[4]  # Column_Name is in column 5
                new_column_name = row[5]  # New_Column_Name is in column 6

                # Add this column to the table's set of columns
                if table_name not in table_columns:
                    table_columns[table_name] = set()  # Create empty set if table is new
                table_columns[table_name].add(column_name)

                # Add this table to the column's set of tables
                if new_column_name not in column_tables:
                    column_tables[new_column_name] = set()  # Create empty set if column is new
                column_tables[new_column_name].add(table_name)

                # Count this column name
                column_counter[new_column_name] += 1

                # Increment our total column counter
                total_columns += 1

        # Report what we found
        report_info(f"Processed {total_columns} total columns")
        report_info(f"Found {len(column_counter)} unique column names")
        report_info(f"Found {len(table_columns)} distinct tables")

        # Return all our analysis in a nice package
        return HeadersAnalysis(
            column_counter=column_counter,
            column_tables=column_tables,
            table_columns=table_columns,
            total_columns=total_columns,
        )
    except FileNotFoundError:
        report_error(f"File not found: {file_path}")
        raise
    except OSError as e:
        report_error(f"IO error reading {file_path}: {e}")
        raise
    except Exception as ex:
        report_error(f"Error processing headers file: {ex}")
        raise ValueError(f"Error processing headers file: {ex}")


def report_column_statistics(analysis: HeadersAnalysis) -> None:
    """
    Creates a report showing how frequently each column name appears.

    This function:
    1. Sorts columns by how many times they appear (most frequent first)
    2. Displays a table with each column name and its count
    3. For columns that appear in only a few tables, lists which tables have them

    Args:
        analysis: The HeadersAnalysis object with our statistics
    """
    report_subsection("Column Name Frequency (Sorted by Count)")

    # Get columns sorted by frequency (most common first)
    # most_common() is a method of Counter that returns (item, count) pairs
    sorted_columns = analysis.column_counter.most_common()

    # Print table header
    report_info(f"{'Column Name':<{COLUMN_NAME_DISPLAY_WIDTH}} | {'Count':<{COUNT_DISPLAY_WIDTH}}")
    report_info("-" * (COLUMN_NAME_DISPLAY_WIDTH + COUNT_DISPLAY_WIDTH + 3))  # +3 for " | "

    # Print each column with its count
    for column_name, count in sorted_columns:
        # Format the output to align in nice columns using string formatting
        # The < symbol means left-aligned, and the number is the field width
        report_info(f"{column_name:<{COLUMN_NAME_DISPLAY_WIDTH}} | {count:<{COUNT_DISPLAY_WIDTH}}")

        # For columns with low frequency, show which tables have this column
        if count <= LOW_FREQUENCY_THRESHOLD:
            tables_with_column = sorted(analysis.column_tables[column_name])
            report_comment(f"   Found in tables: {', '.join(tables_with_column)}")


def report_missing_essential_columns(
    table_columns: Dict[str, Set[str]], essential_columns: List[str] = ESSENTIAL_COLUMNS
) -> None:
    """
    Checks which tables are missing important columns and reports them.

    Every table should have certain essential columns like Date, Title, and Text.
    This function identifies tables that are missing any of these columns.

    Args:
        table_columns: Dictionary mapping table names to their columns
        essential_columns: List of column names that every table should have
    """
    report_subsection(f"Tables Missing Essential Columns ({', '.join(essential_columns)})")

    # List to store tuples of (table_name, list_of_missing_columns)
    missing_essential = []

    # Check each table for missing essential columns
    for table_name, columns in table_columns.items():
        # Create a list of essential columns that aren't in this table
        missing = [col for col in essential_columns if col not in columns]

        # If any essential columns are missing, add this table to our list
        if missing:
            missing_essential.append((table_name, missing))

    # Report the results
    if missing_essential:
        report_info(f"Found {len(missing_essential)} tables missing essential columns:")
        for table_name, missing in sorted(missing_essential):
            report_comment(f"   Table: '{table_name}' - Missing: {', '.join(missing)}")
    else:
        report_info("All tables contain the essential columns (Date, Title, Text)")


def main() -> int:
    """
    Main function that runs the entire analysis process.

    Steps:
    1. Display the program header
    2. Check if the headers file exists
    3. Read and analyze the headers file
    4. Report the column statistics
    5. Report tables missing essential columns

    Returns:
        0 if everything worked correctly, 1 if there was an error
    """
    try:
        # Display header with program information
        report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)
        report_section("Column Name Statistics")

        # Step 1: Check if the headers file exists
        if not check_file_exists(HEADERS_TSV_PATH, f"Error: Headers file not found at '{HEADERS_TSV_PATH}'"):
            return 1  # Exit with error code

        # Step 2: Read and analyze the headers file
        analysis = read_headers_file()

        # Step 3: Report column frequencies
        report_column_statistics(analysis)

        # Step 4: Report tables missing essential columns
        report_missing_essential_columns(analysis.table_columns)

        # Everything worked correctly
        return 0

    except FileNotFoundError:
        # Already reported in check_file_exists or read_headers_file
        return 1
    except OSError as e:
        report_error(f"IO error: {e}")
        return 1
    except ValueError as e:
        report_error(f"Value error: {e}")
        return 1
    except Exception as ex:
        report_error(f"Unexpected error: {ex}")
        return 1


# This runs the main function when the script is executed directly
# sys.exit() passes the return value from main() to the operating system
if __name__ == "__main__":
    sys.exit(main())
