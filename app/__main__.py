"""Application entrypoint"""

# pylint: disable=wrong-import-position

import logging
import pickle
import pathlib
import argparse
import sys

from typing import Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

from .setup import setup
from .teardown import teardown
from .create_tables import create_tables, test_connection
from .etl import (
    copy_to_tables,
    insert_into_tables,
    run_sample_queries as sample_queries,
)

from .models import RedshiftConfig


def run_and_pickle_setup():
    """Run and pickle the setup for later use."""
    redshift_config, role_arn, region = setup()
    pickle_path = pathlib.Path(__file__).parent.parent / "setup_output.pkl"

    with open(pickle_path, "wb") as file:
        pickle.dump((redshift_config, role_arn, region), file)

    logger.info("Setup output saved to %s", pickle_path)


def load_pickled_setup() -> Tuple[RedshiftConfig, str, str]:
    """
    Load the previously pickled Redshift setup output.

    Returns
    -------
    Tuple[RedshiftConfig, str, str]
        (RedshiftConfig, role_arn, region) loaded from pickle.
    """
    pickle_path = pathlib.Path(__file__).parent.parent / "setup_output.pkl"

    logger.info("Loading setup output from %s", pickle_path)

    with open(pickle_path, "rb") as file:
        redshift_config, role_arn, region = pickle.load(file)

    return redshift_config, role_arn, region


def setup_redshift_tables():
    """Run table creation, copy, insert, and validation."""
    redshift_config, role_arn, region = load_pickled_setup()

    logger.info("Attempting connection to %s", redshift_config)
    test_connection(redshift_config, retries=10, delay=5)

    create_tables(redshift_config)
    copy_to_tables(redshift_config, role_arn, region)
    insert_into_tables(redshift_config)

    logger.info("Redshift setup and ETL complete.")


def run_sample_queries():
    """Run the sample queries for the demonstration."""
    redshift_config, _, _ = load_pickled_setup()

    sample_queries(redshift_config)


def run_teardown():
    """
    Run the teardown method to reset the workspace and
    delete the local pickle file storing Redshift setup information.
    """
    teardown()

    pickle_path = pathlib.Path(__file__).parent.parent / "setup_output.pkl"

    if pickle_path.exists():
        pickle_path.unlink()
        logger.info("Deleted pickle file at %s", pickle_path)
    else:
        logger.warning("Pickle file %s not found. Nothing to delete.", pickle_path)


def main():
    """Application entrypoint"""
    parser = argparse.ArgumentParser(
        description="Redshift Setup, ETL, and Teardown Application"
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--setup", action="store_true", help="Run setup and pickle output"
    )
    group.add_argument("--etl", action="store_true", help="Run ETL using pickled setup")
    group.add_argument(
        "--teardown", action="store_true", help="Run teardown to remove resources"
    )
    group.add_argument(
        "--sample", action="store_true", help="Run the sample queries for the demo"
    )

    args = parser.parse_args()

    pickle_path = pathlib.Path(__file__).parent.parent / "setup_output.pkl"

    if args.setup:
        logger.info("Starting setup...")
        run_and_pickle_setup()

    elif args.etl:
        if not pickle_path.exists():
            logger.error("Pickle file not found. Please run --setup first.")
            sys.exit(1)

        logger.info("Starting ETL process...")
        setup_redshift_tables()

    elif args.teardown:
        logger.info("Starting teardown...")
        run_teardown()

    elif args.sample:
        if not pickle_path.exists():
            logger.error("Pickle file not found. Please run --setup and --etl first.")
            sys.exit(1)

        logger.info("Running sample queries...")
        run_sample_queries()

    else:
        logger.error("No valid option provided.")
        sys.exit(1)


if __name__ == "__main__":
    main()
