"""Methods used to create tables and insert, update, and copy data"""

from typing import List

import time
import logging
import psycopg2

from .models import RedshiftConfig

from .sql_queries import (
    DROP_TABLE_STATEMENTS,
    CREATE_TABLE_STATEMENTS,
)

logger = logging.getLogger(__name__)


def test_connection(config: RedshiftConfig, retries: int = 5, delay: int = 10) -> None:
    """
    Test the connection to the Redshift cluster with retry logic.

    Parameters
    ----------
    config : RedshiftConfig
        Configuration object containing Redshift connection parameters.
    retries : int, optional
        Number of retry attempts if the connection fails. Defaults to 5.
    delay : int, optional
        Seconds to wait between retries. Defaults to 10 seconds.

    Raises
    ------
    psycopg2.OperationalError
        If unable to connect after the specified number of retries.
    """
    attempt = 1

    while attempt <= retries:
        try:
            logger.info(
                "Using Redshift connection string: %s", config.redshift_endpoint
            )

            logger.info(
                "Attempting to connect to Redshift (Attempt %d/%d)...", attempt, retries
            )

            conn = psycopg2.connect(
                dbname=config.db_name,
                user=config.username,
                password=config.password,
                host=config.redshift_endpoint,
                port=config.redshift_port,
                connect_timeout=5,  # Short timeout per attempt
            )
            conn.close()
            logger.info("Connection to Redshift successful!")
            return
        except psycopg2.OperationalError as e:
            logger.warning("Connection attempt %d failed: %s", attempt, e)
            attempt += 1

            if attempt <= retries:
                logger.info("Waiting %d seconds before retrying...", delay)
                time.sleep(delay)
            else:
                logger.error(
                    "All connection attempts failed. Redshift may be unavailable."
                )
                raise e


def run_queries(
    queries: List[str],
    config: RedshiftConfig,
    query_type: str = "QUERY",
    retries: int = 3,
    delay: int = 5,
) -> None:
    """
    Run a list of SQL queries against a Redshift cluster with retry logic.

    Parameters
    ----------
    queries : List[str]
        A list of SQL queries (e.g., CREATE, DROP, COPY, SELECT statements) to
        be executed sequentially.
    config : RedshiftConfig
        Configuration object containing Redshift connection parameters.
    query_type : str, optional
        A label for the type of queries being run (e.g., "QUERY", "COPY", "VALIDATE").
        Used only for logging purposes. Defaults to "QUERY".
    retries : int, optional
        Number of retry attempts if a query fails. Defaults to 3.
    delay : int, optional
        Seconds to wait between retries. Defaults to 5 seconds.
    """
    try:
        conn = psycopg2.connect(
            dbname=config.db_name,
            user=config.username,
            password=config.password,
            host=config.redshift_endpoint,
            port=config.redshift_port,
            connect_timeout=10,  # Short connection timeout
        )

        cur = conn.cursor()
        logger.info("Connected to Redshift successfully.")

        for query in queries:
            attempt = 1
            success = False

            while attempt <= retries and not success:
                try:
                    logger.info(
                        "Running %s (Attempt %d/%d):\n%s",
                        query_type,
                        attempt,
                        retries,
                        query.strip(),
                    )

                    cur.execute(query)

                    if cur.description is not None:
                        rows = cur.fetchall()
                        for row in rows:
                            logger.info("Query Result: %s", row)

                    conn.commit()
                    logger.info("%s executed successfully.", query_type)
                    success = True

                except psycopg2.Error as e:
                    logger.warning(
                        "Error executing %s attempt %d: %s", query_type, attempt, e
                    )
                    attempt += 1
                    if attempt <= retries:
                        logger.info("Retrying in %d seconds...", delay)
                        time.sleep(delay)
                    else:
                        logger.error(
                            "All attempts to run %s failed. Moving to next query.",
                            query_type,
                        )

        cur.close()
        conn.close()

        logger.info("All %s commands executed and connection closed.", query_type)

    except psycopg2.Error as e:
        logger.error("Could not connect to Redshift: %s", e)


def create_tables(config: RedshiftConfig):
    """
    Create the tables necessary for the Redshift database schema.

    Parameters
    ----------
    config : RedshiftConfig
        Configuration object containing Redshift connection parameters.
    """
    run_queries(DROP_TABLE_STATEMENTS, config, query_type="DROP")
    run_queries(CREATE_TABLE_STATEMENTS, config, query_type="CREATE")
