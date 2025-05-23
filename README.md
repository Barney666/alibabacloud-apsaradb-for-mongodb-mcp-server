# AlibabaCloud MongoDB MCP Server

AlibabaCloud MongoDB MCP Server serves as a universal interface between OpenAPI and MongoDB databases. 



## Configuration

Download from Github

```shell
git clone https://github.com/aliyun/alibabacloud-mongodb-mcp-server.git
```

Add the following configuration to the MCP client configuration file:

```json
{
  "mcpServers": {
    "alibabacloud-mongodb": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/alibabacloud-mongodb-mcp-server/src/alibabacloud_mongodb_mcp_server",
        "run",
        "server.py"
      ],
      "env": {
        "ALIBABA_CLOUD_ACCESS_KEY_ID": "access_id",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "access_key",
        "MONGODB_CONNECTION_STRING": "mongodb://username:password@ip:port/auth_db"
      },
      "transportType": "sse"
    }
  }
}
```

## Components

### Tools

* `create_db_instance`: Create a MongoDB instance.
* `allocate_public_network_address`: Allocate a public network address for the instance.
* `describe_replica_set_role`: Query connection string and role information of a MongoDB instance.
* `describe_db_instance_attribute`: Query detailed information about a specific MongoDB instance.
* `describe_db_instances`: Queries all MongoDB instances in a region.
* `describe_vpcs`: Query VPC list.
* `describe_vswitches`: Query VSwitch list.
* `describe_available_resource`:  Query available resource(instance class) for MongoDB instances.
* `describe_available_zones`: Query available zones for MongoDB instances.
* `list_databases`: Query the size of all databases in a MongoDB instance.
* `get_top_reusable_space_collections`: Get the top N collections with the most reusable space in MongoDB


### Resources
None

## Environment Variables

MCP Server requires the following environment variables to connect to AlibabaCloud OpenAPI or MongoDB instance:

- `ALIBABA_CLOUD_ACCESS_KEY_ID`: (Required) Access key id for AlibabaCloud account.
- `ALIBABA_CLOUD_ACCESS_KEY_SECRET`: (Required) Access key secret for AlibabaCloud account.
- `MONGODB_CONNECTION_STRING`: (Optional) Connection string of specified MongoDB instance

## Dependencies

- Python 3.10 or higher
- Required packages:
  - mcp >= 1.0.0
  - pymongo >= 4.12.1
  - python-dotenv >= 1.1.0
  - requests >= 2.32.3
  - alibabacloud-dds20151201 >= 9.0.0
  - alibabacloud-vpc20160428 >= 6.11.5
