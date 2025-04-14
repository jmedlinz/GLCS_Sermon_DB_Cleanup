"""
Configuration Settings File

This file contains all the settings and configuration values needed by the application.
It defines file paths, environment settings, and other constants used throughout the program.
"""

import os  # The 'os' module lets us work with files, folders, and environment variables

# The name of our application
APP_NAME = "GLCS Sermon DB Cleanup"

# ---------- Environment Settings ----------
# Environment settings help us run the code differently based on where it's running
# (like using test data during development but real data in production)

# Define possible environment types
DEV_ENV = "DEV"      # Development environment (for writing and testing code)
TEST_ENV = "TEST"    # Testing environment (for formal testing)
PROD_ENV = "PROD"    # Production environment (the "real" system in actual use)

# Set which environment we're currently using
# Change this value to DEV_ENV during development if needed
APP_ENVIRONMENT = PROD_ENV

# ---------- System Information ----------
# Get information about the current user and computer
# Environment variables are special variables provided by your operating system

# Get the username of the person running this program
USERNAME = os.getenv("USERNAME")  # Example: "jmedlin"

# Get the name of the computer this program is running on
COMPUTERNAME = os.getenv("COMPUTERNAME")  # Example: "LAPTOP-ABC123"

# ---------- File Paths ----------
# These settings define where our files are located

# Base directory where all our data files are stored
# The 'r' before the string makes it a "raw string" so backslashes are treated literally
DATA_DIR = r"C:\Users\jmedlin\Documents\GitHub\GLCS_Sermon_DB_Cleanup\data"

# ---------- Database Settings ----------
# Microsoft Access database file information
SOURCE_DB_FILE = "JN Sermons Library_for JMM to edit.accdb"

# Full path to the database file
# os.path.join combines paths correctly regardless of operating system
# (Windows uses backslashes, Mac/Linux use forward slashes)
SOURCE_DB_PATH = os.path.join(DATA_DIR, SOURCE_DB_FILE)
# This creates: "C:\Users\jmedlin\Documents\GitHub\GLCS_Sermon_DB_Cleanup\data\JN Sermons Library_for JMM to edit.accdb"

# ---------- Output File Settings ----------
# Settings for the TSV (Tab-Separated Values) files we'll create

# Master table file (stores information about all tables in the database)
MASTER_TSV_FILE = "master_table.tsv"
MASTER_TSV_PATH = os.path.join(DATA_DIR, MASTER_TSV_FILE)
# This creates: "C:\Users\jmedlin\Documents\GitHub\GLCS_Sermon_DB_Cleanup\data\master_table.tsv"

# Headers/columns file (stores information about table columns)
HEADERS_TSV_FILE = "columns.tsv"
HEADERS_TSV_PATH = os.path.join(DATA_DIR, HEADERS_TSV_FILE)
# This creates: "C:\Users\jmedlin\Documents\GitHub\GLCS_Sermon_DB_Cleanup\data\columns.tsv"
