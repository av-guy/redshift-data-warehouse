"""Programmatic teardown of the resources that we created"""

# pylint: disable=broad-exception-caught

import pathlib
import configparser
import logging
import boto3

logger = logging.getLogger(__name__)

current_dir = pathlib.Path(__file__).parent.parent
config_path = current_dir / "dwh.cfg"

config = configparser.ConfigParser()
config.read(config_path)

ROLE_NAME = config.get("IAM", "ROLE_NAME")
USER_NAME = config.get("IAM", "USER_NAME")
SECURITY_GROUP_NAME = config.get("SECURITY", "SECURITY_GROUP_NAME")
SUBNET_GROUP_NAME = config.get("SECURITY", "SUBNET_GROUP_NAME")
CLUSTER_IDENTIFIER = config.get("CLUSTER", "CLUSTER_IDENTIFIER")

redshift_client = boto3.client("redshift")
iam_client = boto3.client("iam")
ec2_client = boto3.client("ec2")


def delete_redshift_cluster():
    """Delete Redshift Cluster"""
    logger.info("Deleting Redshift Cluster...")
    try:
        redshift_client.delete_cluster(
            ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
        )
        logger.info("Waiting for Redshift Cluster to be deleted...")
        waiter = redshift_client.get_waiter("cluster_deleted")
        waiter.wait(ClusterIdentifier=CLUSTER_IDENTIFIER)
        logger.info("Redshift Cluster deleted.")
    except Exception as e:
        logger.warning("Error encountered while attempting Redshift cluster deletion")
        logger.warning("Error description: %s", e)


def delete_subnet_group():
    """Delete Subnet Group"""
    logger.info("Deleting Cluster Subnet Group...")
    try:
        redshift_client.delete_cluster_subnet_group(
            ClusterSubnetGroupName=SUBNET_GROUP_NAME
        )
        logger.info("Cluster Subnet Group deleted.")
    except Exception as e:
        logger.warning("Error encountered while attempting subnet group deletion")
        logger.warning("Error description: %s", e)


def delete_iam_role():
    """Detach Policies and Delete IAM Role"""
    logger.info("Detaching policies and deleting IAM Role...")
    try:
        iam_client.detach_role_policy(
            RoleName=ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
        iam_client.delete_role(RoleName=ROLE_NAME)
        logger.info("IAM Role deleted.")
    except Exception as e:
        logger.warning("Error encountered while attempting to delete IAM role")
        logger.warning("Error description: %s", e)


def delete_security_group():
    """Delete Security Group"""
    logger.info("Deleting Security Group...")
    try:
        groups = ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [SECURITY_GROUP_NAME]}]
        )
        for group in groups["SecurityGroups"]:
            ec2_client.delete_security_group(GroupId=group["GroupId"])
        logger.info("Security Group deleted.")
    except Exception as e:
        logger.warning("Error encountered while attempting to delete security group")
        logger.warning("Error description: %s", e)


def teardown():
    """Run teardown steps in order"""
    delete_redshift_cluster()
    delete_subnet_group()
    delete_iam_role()
    delete_security_group()

    logger.info(
        "Teardown complete! IAM user still exists; delete from console if necessary"
    )
