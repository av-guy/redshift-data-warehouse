"""Programmatic setup of necessary resources for project"""

import json
import time
import pathlib
import configparser
import logging

from typing import Tuple

import boto3

from .models import RedshiftConfig

logger = logging.getLogger(__name__)

current_dir = pathlib.Path(__file__).parent.parent
config_path = current_dir / "dwh.cfg"

config = configparser.ConfigParser()
config.read(config_path)

REGION = config.get("AWS", "REGION")

ROLE_NAME = config.get("IAM", "ROLE_NAME")
USER_NAME = config.get("IAM", "USER_NAME")

SECURITY_GROUP_NAME = config.get("SECURITY", "SECURITY_GROUP_NAME")
SUBNET_GROUP_NAME = config.get("SECURITY", "SUBNET_GROUP_NAME")
DEFAULT_VPC_ID = config.get("AWS", "DEFAULT_VPC_ID")

CLUSTER_IDENTIFIER = config.get("CLUSTER", "CLUSTER_IDENTIFIER")
DB_NAME = config.get("CLUSTER", "DB_NAME")
MASTER_USERNAME = config.get("CLUSTER", "MASTER_USERNAME")
MASTER_PASSWORD = config.get("CLUSTER", "MASTER_PASSWORD")
NODE_TYPE = config.get("CLUSTER", "NODE_TYPE")
CLUSTER_TYPE = config.get("CLUSTER", "CLUSTER_TYPE")
PORT = config.getint("CLUSTER", "PORT")

iam_client = boto3.client("iam", region_name=REGION)
ec2_client = boto3.client("ec2", region_name=REGION)
redshift_client = boto3.client("redshift", region_name=REGION)


def create_iam_role() -> str:
    """
    Create the IAM role required for Redshift to access S3.

    Returns
    -------
    str
        The Amazon Resource Name (ARN) of the created IAM role.
    """
    logger.info("Creating IAM Role...")
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    role = iam_client.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy),
        Description="Role for Redshift to access S3",
    )

    iam_client.attach_role_policy(
        RoleName=ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )

    role_arn = role["Role"]["Arn"]
    return role_arn


def create_security_group():
    """
    Create a security group allowing Redshift public access in
    the default VPC.

    Returns
    -------
    Tuple[str, str]
        A tuple containing the VPC ID and the Security Group ID created.
    """
    logger.info("Creating Security Group...")

    response = ec2_client.describe_vpcs()
    vpc_id = response["Vpcs"][0]["VpcId"]

    security_group = ec2_client.create_security_group(
        GroupName=SECURITY_GROUP_NAME,
        Description="Security Group for Redshift allowing public access",
        VpcId=vpc_id,
    )

    ec2_client.authorize_security_group_ingress(
        GroupId=security_group["GroupId"],
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": PORT,
                "ToPort": PORT,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )

    return vpc_id, security_group["GroupId"]


def create_subnet_group(vpc_id: str):
    """
    Create a Redshift subnet group associated with the provided VPC.

    Parameters
    ----------
    vpc_id : str
        The ID of the VPC in which to create the subnet group.
    """
    logger.info("Creating Subnet Group...")

    subnets = ec2_client.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )

    subnet_ids = [subnet["SubnetId"] for subnet in subnets["Subnets"]]

    redshift_client.create_cluster_subnet_group(
        ClusterSubnetGroupName=SUBNET_GROUP_NAME,
        Description="Subnet group for Redshift cluster",
        SubnetIds=subnet_ids,
    )

    response = redshift_client.describe_cluster_subnet_groups()

    logger.info(
        "Current Subnet Groups: %s",
        [sg["ClusterSubnetGroupName"] for sg in response["ClusterSubnetGroups"]],
    )


def launch_redshift_cluster(role_arn: str, security_group_id: str) -> str:
    """
    Launch a Redshift cluster using the provided IAM role and security group.

    Parameters
    ----------
    role_arn : str
        The ARN of the IAM role to associate with the Redshift cluster.
    security_group_id : str
        The ID of the security group to attach to the Redshift cluster.

    Returns
    -------
    str
        The endpoint address of the launched Redshift cluster.
    """
    logger.info("Launching Redshift Cluster...")

    response = ec2_client.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [DEFAULT_VPC_ID]},
            {"Name": "group-name", "Values": ["default"]},
        ]
    )

    if not response["SecurityGroups"]:
        raise ResourceWarning("Default Security Group not found in the VPC.")

    default_sg_id = response["SecurityGroups"][0]["GroupId"]
    logger.info("Found Default Security Group ID: %s", default_sg_id)

    vpc_security_group_ids = [security_group_id, default_sg_id]

    redshift_client.create_cluster(
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        NodeType="dc2.large",
        MasterUsername=MASTER_USERNAME,
        MasterUserPassword=MASTER_PASSWORD,
        DBName=DB_NAME,
        ClusterType="single-node",
        VpcSecurityGroupIds=vpc_security_group_ids,
        ClusterSubnetGroupName=SUBNET_GROUP_NAME,
        IamRoles=[role_arn],
        Port=PORT,
        PubliclyAccessible=True,
    )

    logger.info(
        "Waiting for cluster to become available (this may take a few minutes)..."
    )

    waiter = redshift_client.get_waiter("cluster_available")
    waiter.wait(ClusterIdentifier=CLUSTER_IDENTIFIER)

    logger.info("Cluster is now available!")

    cluster_info = redshift_client.describe_clusters(
        ClusterIdentifier=CLUSTER_IDENTIFIER
    )

    endpoint = cluster_info["Clusters"][0]["Endpoint"]["Address"]
    logger.info("Redshift endpoint: %s", endpoint)

    return endpoint


def setup() -> Tuple[RedshiftConfig, str, str]:
    """
    Initialize and set up all necessary AWS resources for Redshift access.

    Creates the IAM role, security group, subnet group, and launches the Redshift cluster.

    Returns
    -------
    Tuple[RedshiftConfig, str, str]
        A tuple containing the RedshiftConfig object, the IAM role ARN, and the AWS region.
    """
    role_arn = create_iam_role()
    vpc_id, security_group_id = create_security_group()

    create_subnet_group(vpc_id)
    logger.info("Waiting 15 seconds for database to come online...")

    time.sleep(15)

    endpoint = launch_redshift_cluster(role_arn, security_group_id)
    logger.info("Setup complete! Redshift is ready at %s", endpoint)

    return (
        RedshiftConfig(
            username=MASTER_USERNAME,
            password=MASTER_PASSWORD,
            redshift_endpoint=endpoint,
            db_name=DB_NAME,
        ),
        role_arn,
        REGION,
    )
