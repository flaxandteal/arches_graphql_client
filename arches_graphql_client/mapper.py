import pandas as pd

from .utils import camel


class Mapper:
    def __init__(self, mapping, computed_fields, mapping_types, type_to_type, skip):
        self.mapping = mapping
        self.computed_fields = computed_fields
        self.mapping_types = mapping_types
        self.type_to_type = type_to_type
        self.skip = skip

    def map_row(self, row):
        mapping = self.mapping
        row = dict(row.items())
        row.update(
            dict(
                {
                    mapping.get(key, key): val
                    for key, val in row.items()
                    if pd.notna(val) and mapping.get(key, key) not in self.skip
                },
                **{key: None for key in self.computed_fields},
            )
        )
        return row

    def _map_field(self, field, value, row):
        if field in self.computed_fields:
            return self.computed_fields[field][0](row)
        return self.mapping_types[field][1](value)

    def get_type(self, field):
        if field in self.computed_fields:
            return self.type_to_type[self.computed_fields[field][1]]
        return self.type_to_type[self.mapping_types[field][0]]

    def get_maplist(self):
        return list(self.mapping.items()) + list(
            zip(self.computed_fields.keys(), self.computed_fields.keys())
        )

    def apply(self, row):
        kwmapping = [v for k, v in self.get_maplist() if v not in self.skip]

        field_set = self.map_row(row)
        values = {
            camel(field): self._map_field(field, value, field_set)
            for field, value in field_set.items()
            if field in kwmapping
        }
        return values

    async def bulk_create_monuments_from_rows(self, client, rows):
        kwmapping = [v for k, v in self.get_maplist() if v not in self.skip]

        mapped = [self.map_row(row) for ix, row in rows.iterrows()]
        values = []
        for n, field_set in enumerate(mapped):
            try:
                values.append(
                    {
                        camel(field): self._map_field(field, value, field_set)
                        for field, value in field_set.items()
                        if field in kwmapping
                    }
                )
            except Exception as exc:
                raise RuntimeError(f"Could not map fieldset {n}: {field_set}") from exc

        return (await client.bulk_create(values))["bulkCreateMonument"]["monuments"]

    async def bulk_create_monuments_from_split_df(self, client, split_df):
        results = []
        for batch, rows in enumerate(split_df):
            print(f"Batch {batch}")
            try:
                bulk_create = await self.bulk_create_monuments_from_rows(client, rows)
                results += bulk_create
            except Exception:
                print(
                    f"Exception in batch {batch} from row {batch * len(rows)} to row {(batch + 1) * len(rows)}"
                )
                raise
        return results
