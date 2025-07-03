import os
import logging
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from alibabacloud_dds20151201.client import Client as Dds20151201Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_vpc20160428.client import Client as Vpc20160428Client
from alibabacloud_sls20201230.client import Client as Sls20201230Client


mcp = FastMCP("apsaradb_mongodb_mcp_server")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("apsaradb_mongodb_mcp_server")

load_dotenv()
global_config = None


def get_mongodb_connection_configuration():
    # Check if configuration is already cached
    global global_config
    if global_config:
        return global_config

    # Use connection string from environment variable
    connection_string = os.getenv("MONGODB_CONNECTION_STRING")
    if connection_string:
        logger.info("Database configuration loaded successfully: connection_string=%s", connection_string)
        global_config = connection_string
        return global_config

    # Use individual configuration parameters
    config = {
        "host": os.getenv("MONGODB_HOST"),
        "port": os.getenv("MONGODB_PORT"),
        "user": os.getenv("MONGODB_USER"),
        "password": os.getenv("MONGODB_PASSWORD"),
        "database": os.getenv("MONGODB_DATABASE"),
    }

    # Check if all required parameters are present
    missing_params = [
        key for key in ["host", "port", "user", "password", "database"] if not config.get(key)
    ]
    if missing_params:
        logger.error(
            "Missing required database configuration. Please check the following parameters: %s",
            ", ".join(missing_params),
        )
        raise ValueError(
            "Unable to obtain database connection configuration information from environment variables. "
            "Please provide database connection configuration information."
        )

    logger.info(
        "Database configuration loaded successfully: host=%s, port=%d, user=%s, database=%s",
        config["host"],
        config["port"],
        config["user"],
        config["database"],
    )
    global_config = config

    return global_config


def get_dds_client() -> Dds20151201Client:
    try:
        config = open_api_models.Config(
            access_key_id=os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
            access_key_secret=os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        )
        config.endpoint = f'mongodb.aliyuncs.com'
        return Dds20151201Client(config)
    except Exception as e:
        logger.error("Failed to create OpenAPI client: %s", str(e))
        raise


def get_vpc_client(region_id: str) -> Vpc20160428Client:
    try:
        config = open_api_models.Config(
            access_key_id=os.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
            access_key_secret=os.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        )
        if region_id:
            config.endpoint = f'vpc.{region_id}.aliyuncs.com'
        return Vpc20160428Client(config)
    except Exception as e:
        logger.error("Failed to create VPC client: %s", str(e))
        raise


def get_interal_sls_client(region_id: str) -> Sls20201230Client:
    try:
        config = open_api_models.Config(
            access_key_id=os.getenv("INTERNAL_ACCESS_KEY_ID"),
            access_key_secret=os.getenv("INTERNAL_ACCESS_KEY_SECRET"),
        )
        if region_id:
            config.endpoint = f'{region_id}.log.aliyuncs.com'
        return Sls20201230Client(config)
    except Exception as e:
        logger.error("Failed to create SLS client: %s", str(e))
        raise
