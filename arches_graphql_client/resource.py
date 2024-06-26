from gql import gql
import logging

from .client import BaseClient
from .utils import camel, studly


logger = logging.getLogger(__name__)


class ResourceClient(BaseClient):
    __url_prefix__ = "resources/"
    client = None

    def __init__(self, root=None, resource_model_name=None, label_field=None):
        super().__init__(root)
        self.resource_model_name = resource_model_name
        self.label_field = label_field

    async def create(self, field_set, do_index=True):
        if self.label_field:
            return_fields = f"{{ id, {camel(self.label_field)} }}"
        else:
            return_fields = "{ id }"

        query = gql(
            f"""
            mutation create{studly(self.resource_model_name)}($input: {studly(self.resource_model_name)}Input, $doIndex: Boolean) {{
                create{studly(self.resource_model_name)}(fieldSet: $input, doIndex: $doIndex) {{
                    ok,
                    {camel(self.resource_model_name)} {return_fields}
                }}
            }}
            """
        )

        logger.debug(f"Creating {self.resource_model_name} (index={do_index})")
        results = await self.client.execute_async(
            query, variable_values={"input": field_set, "doIndex": do_index}
        )
        return results[f"create{studly(self.resource_model_name)}"][
            f"{camel(self.resource_model_name)}"
        ]

    async def bulk_create(self, field_sets, do_index=True):
        if self.label_field:
            return_fields = f"{{ id, {camel(self.label_field)} }}"
        else:
            return_fields = "{ id }"

        query = gql(
            f"""
            mutation bulkCreate{studly(self.resource_model_name)}($input: [{studly(self.resource_model_name)}Input], $doIndex: Boolean) {{
                bulkCreate{studly(self.resource_model_name)}(fieldSets: $input, doIndex: $doIndex) {{
                    ok,
                    {camel(self.resource_model_name)}s {return_fields}
                }}
            }}
            """
        )

        logger.debug(f"Bulk creating {len(field_sets)} {self.resource_model_name} inputs (index={do_index})")
        results = await self.client.execute_async(
            query, variable_values={"input": field_sets, "doIndex": do_index}
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
                  id,
                  {', '.join((f"{ field[0] } {{ { ', '.join(field[1]) } }}" if isinstance(field, tuple) else field) for field in fields)}
                }}
            }}
        """
        )
        return await self.client.execute_async(get_query, variable_values={"id": id})

    async def list(self, fields):
        list_query = gql(
            f"""
            query {{
              {camel(self.resource_model_name)} {{
                id,
                {', '.join(field for field in fields)}
              }}
            }}
        """
        )
        return await self.client.execute_async(
            list_query, variable_values={}
        )

    async def search(self, text, search_fields, fields=None):
        search_query = gql(
            f"""
            query ($text: String, $searchFields: [String]) {{
              search{studly(self.resource_model_name)}(text: $text, fields: $searchFields) {{
                id,
                {', '.join(field for field in fields)}
              }}
            }}
        """
        )
        if not search_fields:
            search_fields = fields
        if not fields:
            fields = search_fields
        return await self.client.execute_async(
            search_query, variable_values={"text": text, "searchFields": search_fields, "fields": fields}
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
