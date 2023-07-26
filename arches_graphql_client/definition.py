import json
from pathlib import Path
import csv
import sys

mapping_types_standardization = {
    "str": "str",
    "bool": "bool",
    "edtf": "edtf",
    "integer": "int",
    "int": "int",
    "text": "str",
    "concept": "concept",
    "date": "date",
    "datetime": "datetime",
    "text": "str",
    "resource": "resource",
    "[resource]": "[resource]",
    "location": "geojson",
    "dropdown": "concept",
    "[text]": "[str]",
    "[concept]": "[concept]",
    "option select": "concept",
}

def update_mapping_new(arches_model, filename):
    model_definition = arches_model['graph'][0]

    nodes = {m["alias"]: m for m in model_definition["nodes"]}
    parents = {m["nodegroupid"]: m["parentnodegroup_id"] for m in model_definition["nodegroups"] if m["parentnodegroup_id"]}
    models = {}
    with open(filename, "r") as fd:
        reader = csv.DictReader(fd)
        for n, row in enumerate(reader):
            if row["type"] not in mapping_types_standardization:
                print("Missing", row["type"])
                continue
            if row["map_name_arches"]:
                node = nodes[row["map_name_arches"].split("/", -1)[-1]]
                typ = mapping_types_standardization[row["type"]]
                models[row["map_name_arches"]] = {
                    "type": typ,
                    "nodeid": node["nodeid"],
                    "nodegroupid": node["nodegroup_id"],
                }
                if typ in ("str", "[str]"):
                    models[row["map_name_arches"]]["lang"] = row.get("Language", "en")
                assert node["nodeid"] == row["nodeid"]
                assert node["nodegroup_id"] == row["nodegroup_id"]
                if row["nodegroup_id"] in parents:
                    models[row["map_name_arches"]]["parentnodegroup_id"] = parents[node["nodegroup_id"]]
            else:
                print("map_name_arches missing in row", n)

    models["graphid"] = model_definition["graphid"]
    return models

def add_model(model, mapping_file, api_file, model_folder="./pkg/graphs/resource_models/"):
    with (Path(model_folder) / f"{model}.json").open("r") as f:
        arches_model = json.load(f)
    if api_file.exists:
        with filename_out.open("r") as fd:
            models = json.load(fd)
    else:
        models = {}
    models[model] = update_mapping_new(arches_model, mapping_file)
    with api_file.open("w") as fd:
        json.dump(models, fd)

if __name__ == "__main__":
    model = sys.argv[3]
    filename_out = Path(sys.argv[2])
    filename_in = Path(sys.argv[1])
    add_model(model, filename_in, filename_out)
