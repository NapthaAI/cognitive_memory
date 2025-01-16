#!/usr/bin/env python
from datetime import datetime, timezone
from dotenv import load_dotenv
import random
from typing import Dict, Any
from naptha_sdk.schemas import MemoryDeployment, MemoryRunInput
from naptha_sdk.storage.schemas import CreateStorageRequest, ReadStorageRequest, DeleteStorageRequest, ListStorageRequest, DatabaseReadOptions
from naptha_sdk.storage.storage_provider import StorageProvider
from naptha_sdk.user import sign_consumer_id
from naptha_sdk.utils import get_logger
from cognitive_memory.schemas import InputSchema

load_dotenv()

logger = get_logger(__name__)

class CognitiveMemory():
    """
    Handles storing and retrieving single-step memory items in agent_{agent_id}_cognitive table.
    """
    def __init__(self, deployment: Dict[str, Any]):
        self.deployment = deployment
        self.config = self.deployment.config
        self.storage_provider = StorageProvider(self.deployment.node)
        self.storage_type = self.config.storage_type
        self.table_name = self.config.path
        self.schema = self.config.schema

    # TODO: Remove this. In future, the create function should be called by create_module in the same way that run is called by run_module
    async def init(self, *args, **kwargs):
        await create(self.deployment)
        return {"status": "success", "message": f"Successfully populated {self.table_name} table"}

    async def store_cognitive_item(self, input_data: Dict[str, Any], *args, **kwargs):
        """
        Insert a single cognitive step into the 'cognitive' table,
        returning the DB's actual created_at timestamp.
        """
        logger.info(f"Adding {(input_data)} to table {self.table_name}")

        # if row has no id, generate a random one
        if 'id' not in input_data:
            input_data['id'] = random.randint(1, 1000000)

        if 'created_at' not in input_data:
            input_data['created_at'] =  str(datetime.now(timezone.utc))

        create_row_result = await self.storage_provider.execute(CreateStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            data={"data": input_data}
        ))
        logger.info(f"Create row result: {create_row_result}")

        logger.info(f"Successfully added {input_data} to table {self.table_name}")
        return {"status": "success", "message": f"Successfully added {input_data} to table {self.table_name}"}

    async def get_cognitive_items(self, input_data: Dict[str, Any], *args, **kwargs):
        """
        Retrieve a list of single-step memory items (short-term) from the agent's cognitive table.
        """
        logger.info(f"Querying table {self.table_name} with query: {input_data}")

        read_storage_request = ReadStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"conditions": [input_data]}
        )

        read_result = await self.storage_provider.execute(read_storage_request)
        print(f"Query results: {read_result}")
        return {"status": "success", "message": f"Query results: {read_result}"}


    async def delete_cognitive_items(self, input_data: Dict[str, Any], *args, **kwargs):
        """
        Delete rows from the short-term cognitive memory based on filters.
        Returns how many rows were deleted.
        """
        delete_row_request = DeleteStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"condition": input_data['condition']}
        )

        delete_row_result = await self.storage_provider.execute(delete_row_request)
        logger.info(f"Delete row result: {delete_row_result}")
        return {"status": "success", "message": f"Delete row result: {delete_row_result}"}

    async def delete_table(self, input_data: Dict[str, Any], *args, **kwargs):
        delete_table_request = DeleteStorageRequest(
            storage_type=self.storage_type,
            path=input_data['table_name'],
        )
        delete_table_result = await self.storage_provider.execute(delete_table_request)
        logger.info(f"Delete table result: {delete_table_result}")
        return {"status": "success", "message": f"Delete table result: {delete_table_result}"}


# TODO: Make it so that the create function is called when the memory/create endpoint is called
async def create(deployment: MemoryDeployment):
    """
    Create the Cognitive Memory table
    Args:
        deployment: Deployment configuration containing deployment details
    """
    storage_provider = StorageProvider(deployment.node)
    storage_type = deployment.config.storage_type
    table_name = deployment.config.path
    schema = {"schema": deployment.config.schema}

    logger.info(f"Creating {storage_type} at {table_name} with schema {schema}")


    create_table_request = CreateStorageRequest(
        storage_type=storage_type,
        path=table_name,
        data=schema
    )

    # Create a table
    create_table_result = await storage_provider.execute(create_table_request)

    logger.info(f"Result: {create_table_result}")
    return {"status": "success", "message": f"Successfully created {table_name}"}

# Default entrypoint when the module is executed
async def run(module_run: Dict):
    module_run = MemoryRunInput(**module_run)
    module_run.inputs = InputSchema(**module_run.inputs)
    cognitive_memory = CognitiveMemory(module_run.deployment)
    method = getattr(cognitive_memory, module_run.inputs.func_name, None)
    return await method(module_run.inputs.func_input_data)

if __name__ == "__main__":
    import asyncio
    import os
    from naptha_sdk.client.naptha import Naptha
    from naptha_sdk.configs import setup_module_deployment

    naptha = Naptha()

    deployment = asyncio.run(setup_module_deployment("memory", "cognitive_memory/configs/deployment.json", node_url = os.getenv("NODE_URL")))

    inputs_dict = {
        "init": {
            "func_name": "init",
            "func_input_data": None,
        },
        "add_data": {
            "func_name": "store_cognitive_item",
            "func_input_data": {
                "cognitive_step": "reflection",
                "content": "I am reflecting."
            },
        },
        "run_query": {
            "func_name": "get_cognitive_items",
            "func_input_data": {"cognitive_step": "reflection"},
        },
        "delete_table": {
            "func_name": "delete_table",
            "func_input_data": {"table_name": "cognitive_memory"},
        },
        "delete_row": {
            "func_name": "delete_cognitive_items",
            "func_input_data": {"condition": {"cognitive_step": "reflection"}},
        },
    }

    module_run = {
        "inputs": inputs_dict["init"],
        "deployment": deployment,
        "consumer_id": naptha.user.id,
        "signature": sign_consumer_id(naptha.user.id, os.getenv("PRIVATE_KEY"))
    }

    response = asyncio.run(run(module_run))
