"""This handles the command-line interface (CLI) for the ETL (Extract, Transform, Load) process and querying."""

import sys
import argparse
from lib.extract import extract
from lib.query import query
from lib.transform_load import load


def handle_arguments(args):
    """Add action based on initial calls."""
    parser = argparse.ArgumentParser(
        description="ETL-Query script for WR Rankings Data"
    )
    parser.add_argument(
        "action",
        choices=["extract", "load", "query"],
        help="Specify the action to perform: extract, load, or query",
    )

    if "query" in args:
        parser.add_argument("query", help="SQL query to execute")

    return parser.parse_args(args)


def main():
    """Handles all the CLI commands."""
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        print("Extracting data...")
        extract()  # Adjust this to accept the correct URLs and paths if needed
    elif args.action == "load":
        print("Transforming and loading data...")
        load()  # Ensure this is set to load the correct datasets
    elif args.action == "query":
        print("Executing query...")
        query(args.query)  # Make sure the query function can handle the input correctly
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
