"""Model used throughout the program"""

from dataclasses import dataclass


@dataclass
class RedshiftConfig:
    """
    Configuration object for connecting to an Amazon Redshift cluster.

    Attributes
    ----------
    username : str
        The username used to authenticate with the Redshift cluster.
    password : str
        The password associated with the Redshift username.
    redshift_endpoint : str
        The endpoint URL of the Redshift cluster
        (e.g., 'your-cluster-name.region.redshift.amazonaws.com').
    db_name : str
        The name of the database within the Redshift cluster to connect to (e.g., 'dev').
    redshift_port : int, optional
        The port used to connect to Redshift. Defaults to 5439.
    """

    username: str
    password: str
    redshift_endpoint: str
    db_name: str
    redshift_port: int = 5439
