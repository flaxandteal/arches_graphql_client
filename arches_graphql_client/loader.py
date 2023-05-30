import re
from functools import partial, cache
import csv
import shapely.geometry, shapely.ops
import pandas as pd
from pyproj import CRS, Transformer
import dateparser
import geojson
from functools import partial
import pandas as pd
import dateparser

from .mapper import Mapper
from .utils import studly


def _transformer(remap):
        igr = CRS(remap[0])
        wgs84 = CRS(remap[1])
        transformer = Transformer.from_crs(igr, wgs84, always_xy=True)
        return transformer

@cache
def _caching_transformer(remap):
    return _transformer(remap)


def naive_date_to_date_string(date):
    return dateparser.parse(date).strftime("%Y-%m-%d")


def naive_date_to_isoformat(date):
    return dateparser.parse(date).isoformat()


def shape_to_geojson(shape, remap=None, skip_cache=False):
    if not shape or pd.isna(shape):
        return None
    # would be best to cache but for now it's throwing pickling error
    if remap:
        transformer = (transformer if skip_cache else _caching_transformer)(remap)
        destination = shapely.ops.transform(transformer.transform, shape)
    else:
        destination = shape
    transformed = geojson.FeatureCollection([geojson.Feature(geometry=destination)])
    return geojson.dumps(transformed)


mapping = {}
mapping_types_standardization = {
    "text": "str",
    "boolean": "bool",
    "dropdown": "concept",
    "location": "location",
    "option select": "concept",
    "[text]": "[str]",
}
fields_to_coalesce = {}


def _coalesce_to_array(row, target, cb):
    return [
        val
        for val in [
            cb(row[field])
            for field in fields_to_coalesce[target]
            if pd.notna(row[field])
        ]
        if val is not None
    ]


class Loader:
    types = None

    def __init__(self, location_remap=None):
        self.location_remap = location_remap
        if location_remap:
            _caching_transformer(location_remap)
        self._set_types()

    def _set_types(self):
        self.types: dict = dict(
            {
                "int": int,
                "str": str,
                "tuple": tuple,
                "list": list,
                "dict": dict,
                "bool": bool,
                "edtf": str,
                "date": naive_date_to_date_string,
                "datetime": naive_date_to_isoformat,
                "location": lambda *args, **kwargs: shape_to_geojson(
                    *args, remap=self.location_remap, **kwargs
                ),
            }
        )

    def mapping_to_mapper(
        self,
        arches_field_name,
        source_field_name,
        fields,
        computed_fields,
        concept_lookup,
        skip,
    ):
        mapping_types = {}
        for row in fields:
            if (
                row[arches_field_name] in computed_fields
                and row[arches_field_name] not in fields_to_coalesce
            ):
                continue
            if "/" in row[arches_field_name]:
                field, _ = row[arches_field_name].split("/", -1)
                assert (
                    "/" not in field
                ), "Can only handle one level of grouping (semantic field)"
                assert (
                    field in computed_fields
                ), f"Cannot automatically ingest semantic fields, you must create a computed field for {field}"
                continue

            typ = re.sub("\[(.*)\]", r"\1", row["type"])
            is_array = typ != row["type"]
            typ = mapping_types_standardization.get(typ, typ) or "str"

            if typ == "concept":
                typ = row[arches_field_name]
                mapping_cb = concept_lookup[row[arches_field_name]]
            elif typ in self.types:
                mapping_cb = self.types[typ]
            else:
                print(f"Skipping unknown type: {typ} for {row[source_field_name]}")

            if is_array:
                typ = f"[{typ}]"
                fields_to_coalesce.setdefault(row[arches_field_name], [])
                fields_to_coalesce[row[arches_field_name]].append(
                    row[source_field_name]
                )
                if row[arches_field_name] not in computed_fields:
                    computed_fields[row[arches_field_name]] = (
                        partial(
                            _coalesce_to_array,
                            target=row[arches_field_name],
                            cb=mapping_cb,
                        ),
                        typ,
                    )
                else:
                    assert (
                        computed_fields[row[arches_field_name]][1] == typ
                    ), f"{row['map_name_arches']} {typ}"
            else:
                mapping_types[row[arches_field_name]] = (typ, mapping_cb)

        mapping = {}
        for row in fields:
            if row[arches_field_name] != "" and "/" not in row[arches_field_name]:
                mapping[row[source_field_name]] = row[arches_field_name]

        type_to_type = {
            "str": "String",
            "date": "String",
            "datetime": "String",
            "location": "JSONString",
        }
        for concept in concept_lookup:
            type_to_type[concept] = studly(concept)
            type_to_type[f"[{concept}]"] = f"[{studly(concept)}]"

        mapper = Mapper(mapping, computed_fields, mapping_types, type_to_type, skip)
        return mapper
