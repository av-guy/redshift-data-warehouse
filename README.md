# Redshift Data Warehouse Project

## Project Overview

This project programmatically sets up a fully functional AWS Redshift data warehouse environment.

It covers the following major tasks:

- Create necessary AWS resources (IAM Role, Security Group, Subnet Group, Redshift Cluster)
- Copy raw event and song JSON data from S3 into Redshift staging tables
- Transform and load data from staging tables into a star-schema optimized data model
- Validate the record counts for each table to ensure correct data loading
- Provide easy teardown of all AWS resources after completion

The goal is to demonstrate a full pipeline for building and managing a cloud-based data warehouse solution.

## Repository Structure

The following information outlines the file/folder structure for the `app` directory

| File/Folder        | Description                                                                             |
| :----------------- | :-------------------------------------------------------------------------------------- |
| `models.py`        | Contains the `RedshiftConfig` dataclass for connection parameters.                      |
| `setup.py`         | Handles AWS resource creation (IAM Role, Security Group, Subnet Group, Cluster).        |
| `teardown.py`      | Removes AWS resources and cleans up the workspace.                                      |
| `etl.py`           | Handles SQL execution to copy and insert data.                                          |
| `create_tables.py` | Manages Redshift table creation and validation processes.                               |
| `sql_queries.py`   | Stores SQL queries for table creation, data insertion, copying from S3, and validation. |
| `__main__.py`      | Main entrypoint to control setup, ETL, and teardown via command-line flags.             |

## Configuration File

Before running the application, you must create a `dwh.cfg` file in the **root directory** (outside of the `app/` folder).

This file contains AWS credentials and Redshift configuration information required for the setup scripts.

Example `dwh.cfg` structure:

```ini
[AWS]
KEY = YOUR_AWS_ACCESS_KEY
SECRET = YOUR_AWS_SECRET_KEY
REGION = us-west-2
DEFAULT_VPC_ID = vpc-xxxxxxxx

[IAM]
ROLE_NAME = RedshiftS3ReadOnlyRole
USER_NAME = RedshiftCLIUser

[SECURITY]
SECURITY_GROUP_NAME = RedshiftPublicSG
SUBNET_GROUP_NAME = RedshiftSubnetGroup

[CLUSTER]
CLUSTER_IDENTIFIER = redshift-cluster-1
DB_NAME = dev
MASTER_USERNAME = awsuser
MASTER_PASSWORD = YourStrongPassword1!
NODE_TYPE = dc2.large
CLUSTER_TYPE = single-node
PORT = 5439
```

## How to Run

1. **Clone the repository**

```bash
git clone <repo_url>
cd <repo_name>
```

2. **Create and activate a virtual environment**

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Configure AWS credentials**

Make sure your AWS credentials are available via:

- `aws configure`
- OR setting environment variables
- OR through IAM role permissions if running inside AWS services

5. **Run the app using flags**

This project uses command-line flags to control different stages:

| Command                    | Action                                                                        |
| :------------------------- | :---------------------------------------------------------------------------- |
| `python -m app --setup`    | Create AWS resources (IAM, Security Group, Redshift Cluster) and save setup.  |
| `python -m app --etl`      | Connect to Redshift, create tables, copy data, insert data, validate results. |
| `python -m app --teardown` | Destroy AWS resources and clean up workspace.                                 |
| `python -m app --sample`   | Run the sample queries for the demo.                                          |
| `python -m app --help`     | List the help documentation for the demo.                                     |

**`python -m app --setup` MUST be run before `python -m app --etl` and `python -m app --sample`**

### Example:

```bash
python -m app --setup
python -m app --etl
python -m app --teardown
```

---

## Example Queries

After setting up and populating the Redshift cluster, you can run the following example queries:

### Top 10 Most Played Songs

```sql
SELECT
    s.title AS song_title,
    a.name AS artist_name,
    COUNT(sp.songplay_id) AS play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY s.title, a.name
ORDER BY play_count DESC
LIMIT 10;
```

This query retrieves the top 10 songs based on how many times they were played, including the artist name.

---

### User Level Distribution

```sql
SELECT
    level,
    COUNT(DISTINCT user_id) AS user_count
FROM users
GROUP BY level
ORDER BY user_count DESC;
```

This query shows the distribution of users based on their subscription level (e.g., free vs. paid).

---

# Additional Information

## Redshift Cluster Setup and Access Configuration Summary

### 1. Create an IAM Role

- **Create IAM Role**: Designed for AWS services.
- **Trusted Entity**: Redshift service.
- **Attach Policy**: `AmazonS3ReadOnlyAccess` to allow Redshift to read from S3.
- **Purpose**: Enables Redshift cluster to load data directly from S3 without static credentials.

### 2. Create a Security Group

- **Create Security Group**: From EC2 console.
- **Allow Inbound/Outbound**: Port 5439 (Redshift's default port).
- **Source**: `0.0.0.0/0` (open to the internet for classroom/testing purposes).
- **Attach to**: Default VPC.

### 3. Create an IAM User for AWS CLI and boto3 Access

- **Create IAM User**: With programmatic access.
- **Attach Policies**:
  - `AmazonS3ReadOnlyAccess`
  - `AmazonRedshiftFullAccess`
  - `AmazonEC2FullAccess`
  - `IAMFullAccess`
- **Configure AWS CLI**: Using `aws configure` with the generated Access Key and Secret Key.

### 4. Create a Cluster Subnet Group

- **Create Subnet Group**: Define which subnets Redshift can use within the default VPC.
- **Select Subnets**: From the default VPC.
- **Purpose**: Dictates where the Redshift nodes are launched inside the VPC.

### 5. Launch the Redshift Cluster

- **Create Cluster**:
  - Set master username and password.
  - Attach the IAM role for S3 access.
  - Assign the default VPC.
  - Attach both the default VPC Security Group and the custom Redshift Security Group.
  - Use the Cluster Subnet Group.
  - Ensure **Public Accessibility** is set to **Yes**.
  - No Elastic IP address configuration necessary.

## Additional Notes

- **Public Access Warning**: Allowing `0.0.0.0/0` is acceptable for classroom/testing purposes but should not be used in production.
- **Teardown** your AWS resources after finishing to avoid unwanted costs.
