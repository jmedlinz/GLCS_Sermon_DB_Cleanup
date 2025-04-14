"""
Main entry point for the GLCS Sermon DB Cleanup application.

This script serves as the entry point that orchestrates the execution of other scripts
in the proper sequence.
"""

import sys

from . import column_stats, create_file_of_headers, create_master_table


def main():
    """
    Execute the main application workflow in sequence:
    1. Create the master table from the Access database
    2. Create the headers file with column information
    3. Generate column statistics and reports
    """
    print("GLCS Sermon DB Cleanup - Starting workflow")

    # Step 1: Create master table
    create_master_table.main()

    # Step 2: Create headers file
    create_file_of_headers.main()

    # Step 3: Generate column statistics
    column_stats.main()

    print("GLCS Sermon DB Cleanup - Workflow complete")


if __name__ == "__main__":
    sys.exit(main() or 0)
