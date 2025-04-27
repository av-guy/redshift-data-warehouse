"""Module containing all functions necessary for ETL processes"""

from .models import RedshiftConfig
from .create_tables import run_queries
from .sql_queries import COPY_STATEMENTS, INSERT_STATEMENTS, SAMPLE_QUERIES


def copy_to_tables(config: RedshiftConfig, role_arn: str, region: str):
    """
    Bulk copy data into Redshift staging tables from S3.

    Parameters
    ----------
    config : RedshiftConfig
        Configuration object containing Redshift connection parameters.
    role_arn : str
        The Amazon Resource Name (ARN) of the IAM role that grants Redshift read access to S3.
    region : str
        The AWS region where the S3 bucket is located (e.g., 'us-west-2').
    """
    copy_stmts_fmt = [stmt.format(role_arn, region) for stmt in COPY_STATEMENTS]
    run_queries(copy_stmts_fmt, config, query_type="COPY")


def insert_into_tables(config: RedshiftConfig):
    """
    Insert data from staging tables into fact and dimension tables.

    Parameters
    ----------
    config : RedshiftConfig
        Configuration object containing Redshift connection parameters.
    """
    run_queries(INSERT_STATEMENTS, config, query_type="INSERT")


def run_sample_queries(config: RedshiftConfig):
    """
    Run example analytic queries on the populated Redshift star schema.

    Parameters
    ----------
    config : RedshiftConfig
        Configuration object containing Redshift connection parameters.
    """
    run_queries(SAMPLE_QUERIES, config, query_type="ANALYTICS")
