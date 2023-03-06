from gql import gql

from .client import BaseClient
from .utils import camel, studly


class ResourceClient(BaseClient):
    __url_prefix__ = "resources/"
    client = None

    def __init__(self, root, resource_model_name, label_field):
        super().__init__(root)
        self.resource_model_name = resource_model_name
        self.label_field = label_field

    async def bulk_create(self, field_sets):
        query = gql(
            f"""
            mutation bulkCreate{studly(self.resource_model_name)}($input: [{studly(self.resource_model_name)}Input]) {{
                bulkCreate{studly(self.resource_model_name)}(fieldSets: $input) {{
                    ok,
                    {camel(self.resource_model_name)}s {{ id, {camel(self.label_field)} }}
                }}
            }}
            """
        )

        results = await self.client.execute_async(
            query, variable_values={"input": field_sets}
        )
        return results[f"bulkCreate{studly(self.resource_model_name)}"][
            f"{camel(self.resource_model_name)}s"
        ]

    async def get(self, id, fields=None):
        if not fields:
            fields = [self.label_field]
        get_query = gql(
            f"""
            query ($id: UUID!) {{
                get{studly(self.resource_model_name)} (id: $id) {{
                    {', '.join(field for field in fields)}
                }}
            }}
        """
        )
        return await self.client.execute_async(get_query, variable_values={"id": id})

    async def search(self, text, fields):
        search_query = gql(
            f"""
            query ($text: String, $fields: [String]) {{
              search{studly(self.resource_model_name)}(text: $text, fields: $fields) {{
                  id
                  {camel(self.resource_model_name)}
              }}
            }}
        """
        )
        return await self.client.execute_async(
            search_query, variable_values={"text": text, "fields": fields}
        )

    # Execute the query on the transport
    async def delete(self, id):
        delete_query = gql(
            f"""
            mutation ($id: UUID) {{
              delete{studly(self.resource_model_name)}(id: $id) {{
                ok
              }}
            }}
        """
        )
        return await self.client.execute_async(delete_query, variable_values={"id": id})
