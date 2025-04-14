"""
This script processes a headers TSV file to analyze column statistics. It counts the occurrences
of each unique New_Column_Name, identifies the tables they appear in, and reports the results.
Additionally, it checks for tables missing essential columns and provides detailed insights
into column usage across tables.
"""

import csv
import os
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Dict, List, Set

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

# Constants
MIN_COLUMN_COUNT = 6  # Minimum number of columns expected in the TSV file
LOW_FREQUENCY_THRESHOLD = 5  # Show tables for columns that appear this many times or less
COLUMN_NAME_DISPLAY_WIDTH = 50  # Width for displaying column names
COUNT_DISPLAY_WIDTH = 10  # Width for displaying counts

# Configuration
ESSENTIAL_COLUMNS = ["Date", "Title", "Text"]


@dataclass
class HeadersAnalysis:
    """Data container for header analysis results."""

    column_counter: Counter
    column_tables: Dict[str, Set[str]]
    table_columns: Dict[str, Set[str]]
    total_columns: int


def check_file_exists(file_path: str) -> bool:
    """
    Check if the specified file exists.

    Args:
        file_path: Path to the file to check

    Returns:
        bool: True if file exists, False otherwise
    """
    if not os.path.exists(file_path):
        report_error(f"Error: Headers file not found at '{file_path}'")
        return False
    return True


def read_headers_file(file_path: str = HEADERS_TSV_PATH) -> HeadersAnalysis:
    """
    Read the headers file and analyze column data.

    Args:
        file_path: Path to the headers file

    Returns:
        HeadersAnalysis object containing:
            - Counter of column name frequencies
            - Dictionary mapping column names to tables they appear in
            - Dictionary mapping table names to their columns
            - Total number of columns processed

    Raises:
        FileNotFoundError: If the file doesn't exist
        IOError: If there's an error reading the file
        ValueError: If the file format is invalid
    """
    report_subsection("Reading headers file")
    column_counter = Counter()
    total_columns = 0
    # Track which tables each column appears in
    column_tables = {}
    # Create a dictionary to track columns by table
    table_columns = {}

    try:
        with open(file_path, encoding="utf-8") as tsv_file:
            reader = csv.reader(tsv_file, delimiter="\t")
            next(reader)  # Skip header row

            for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header row
                if len(row) < MIN_COLUMN_COUNT:
                    report_error(f"Row {row_num} has fewer than {MIN_COLUMN_COUNT} columns. Skipping.")
                    continue

                row[0]
                table_name = row[1]
                column_name = row[4]  # Column_Name is in the 5th column
                new_column_name = row[5]  # New_Column_Name is in the 6th column

                # Track columns by table
                if table_name not in table_columns:
                    table_columns[table_name] = set()
                table_columns[table_name].add(column_name)

                # Track tables by column name
                if new_column_name not in column_tables:
                    column_tables[new_column_name] = set()
                column_tables[new_column_name].add(table_name)

                column_counter[new_column_name] += 1
                total_columns += 1

        report_info(f"Processed {total_columns} total columns")
        report_info(f"Found {len(column_counter)} unique column names")
        report_info(f"Found {len(table_columns)} distinct tables")

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
    Report column name frequency statistics.

    Args:
        analysis: HeadersAnalysis object with column statistics
    """
    report_subsection("Column Name Frequency (Sorted by Count)")

    # Get sorted items (most frequent first)
    sorted_columns = analysis.column_counter.most_common()

    # Print table header
    report_info(f"{'Column Name':<{COLUMN_NAME_DISPLAY_WIDTH}} | {'Count':<{COUNT_DISPLAY_WIDTH}}")
    report_info("-" * (COLUMN_NAME_DISPLAY_WIDTH + COUNT_DISPLAY_WIDTH + 3))  # +3 for " | "

    # Print each column with its count
    for column_name, count in sorted_columns:
        # Base display without percentage
        report_info(f"{column_name:<{COLUMN_NAME_DISPLAY_WIDTH}} | {count:<{COUNT_DISPLAY_WIDTH}}")

        # For columns with low frequency, show which tables have this column
        if count <= LOW_FREQUENCY_THRESHOLD:
            tables_with_column = sorted(analysis.column_tables[column_name])
            report_comment(f"   Found in tables: {', '.join(tables_with_column)}")


def report_missing_essential_columns(
    table_columns: Dict[str, Set[str]], essential_columns: List[str] = ESSENTIAL_COLUMNS
) -> None:
    """
    Report tables missing essential columns.

    Args:
        table_columns: Dictionary mapping table names to their columns
        essential_columns: List of column names considered essential
    """
    report_subsection(f"Tables Missing Essential Columns ({', '.join(essential_columns)})")
    missing_essential = []

    for table_name, columns in table_columns.items():
        missing = [col for col in essential_columns if col not in columns]
        if missing:
            missing_essential.append((table_name, missing))

    if missing_essential:
        report_info(f"Found {len(missing_essential)} tables missing essential columns:")
        for table_name, missing in sorted(missing_essential):
            report_comment(f"   Table: '{table_name}' - Missing: {', '.join(missing)}")
    else:
        report_info("All tables contain the essential columns (Date, Title, Text)")


def main() -> int:
    """
    Main function to orchestrate the process.

    Returns:
        int: Exit code (0 for success, 1 for error)
    """
    try:
        report_header(APP_NAME, COMPUTERNAME, APP_ENVIRONMENT, USERNAME)
        report_section("Column Name Statistics")

        # Check if the headers file exists
        if not check_file_exists(HEADERS_TSV_PATH):
            return 1

        # Read and analyze the headers file
        analysis = read_headers_file()

        # Report the column statistics
        report_column_statistics(analysis)

        # Report tables missing essential columns
        report_missing_essential_columns(analysis.table_columns)

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


if __name__ == "__main__":
    sys.exit(main())
