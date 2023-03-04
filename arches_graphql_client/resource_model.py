import json

from gql import gql

from .client import BaseClient
from .utils import camel, studly


class ResourceModelClient(BaseClient):
    __url_prefix__ = "resource-models/"
    client = None

    async def get_available_resource_models(self):

        get_query = gql(
            """
            query {
                getAvailableGraphs
            }
        """
        )
        return await self.client.execute_async(get_query)

    async def get_graph(self, resource_model):
        get_graph_query = gql(
            """
            query ($resourceModel: String) {
                buildGraph(resourceModel: $resourceModel)
            }
        """
        )
        retval = await self.client.execute_async(
            get_graph_query, variable_values={"resourceModel": resource_model}
        )
        graph = json.loads(retval["buildGraph"])
        return graph
