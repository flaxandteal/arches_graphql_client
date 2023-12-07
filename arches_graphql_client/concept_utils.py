import hashlib
import uuid
from collections.abc import Iterable, Mapping

def _cuuid(string):
    hsh = hashlib.md5()
    hsh.update(string.encode("utf-8"))
    return uuid.UUID(hsh.hexdigest())

def extend_skos_consistently(concept_xml_f, collection_xml_f, collection_label, types: Mapping[str, Iterable[str] | None] | Iterable[str]):
    """Add a one or two level list into a concept or collection.

    Takes an open concept XML and collection XML, along with a parent concept label, and collection label,
    and inserts an iterable of concepts into each. If this iterable is a mapping from parent to children,
    then it will add two levels.

    Example:
        {"Townland": ["Place A", "Place B"], "County": ["County A", "County B"]}
        or
        ["Place A", "Place B", "Place C"]
    """
    from rdflib import Graph, Literal, Namespace, RDF, URIRef
    from rdflib.namespace import SKOS, DCTERMS
    from urllib.parse import urlparse, urlunparse
    import json
    graph = Graph()
    graph.parse(data=concept_xml_f.read(), format="application/rdf+xml")

    sample_node: tuple | None = None
    identifier: str | None = None
    for scheme, v, o in graph.triples((None, RDF.type, SKOS.ConceptScheme)):
        identifier = str(scheme)
        for s, v, o in graph.triples((None, SKOS.inScheme, scheme)):
            for predicate, object in graph.predicate_objects(subject=s):
                if predicate == SKOS.prefLabel:
                    sample_node = (s, v, o)
                    break

    if identifier is None:
        raise ValueError("No triples found in concept graph")

    if sample_node is None:
        raise ValueError("No concept could not be found")

    ARCHES = list(urlparse(sample_node[0]))
    ARCHES[2] = "/"
    print(ARCHES)
    ARCHES = Namespace(urlunparse(ARCHES))

    cgraph = Graph()
    cgraph.parse(data=collection_xml_f.read(), format="application/rdf+xml")

    collection_node = None
    for scheme, v, o in cgraph.triples((None, RDF.type, SKOS.Collection)):
        for object in cgraph.objects(subject=scheme, predicate=SKOS.prefLabel):
            if collection_label == json.loads(object.value)["value"]:
                collection_node = (scheme, v, o)

    if collection_node is None:
        raise ValueError("The requested collection could not be matched")

    pairs: Iterable[tuple[str, Iterable[str] | None]]
    if isinstance(types, Mapping):
        pairs = types.items()
    else:
        pairs = iter((name, None) for name in types)

    for name, lst in pairs:
        parent_uuid = str(_cuuid(identifier + name))
        for scheme, v, o in graph.triples((None, RDF.type, SKOS.ConceptScheme)):
            for s, v, o in graph.triples((None, SKOS.inScheme, scheme)):
                for predicate, object in graph.predicate_objects(subject=s):
                    if predicate == SKOS.prefLabel:
                        if name == str(object.value):
                            raise RuntimeError("May already have been run")

        graph.add((ARCHES[parent_uuid], SKOS.prefLabel, Literal(json.dumps({
            "id": str(_cuuid(identifier + name + "label")),
            "value": name
        }), lang="en")))

        graph.add((URIRef(identifier), SKOS.hasTopConcept, ARCHES[parent_uuid]))
        graph.add((ARCHES[parent_uuid], RDF.type, SKOS.Concept))
        graph.add((ARCHES[parent_uuid], DCTERMS.identifier, Literal(json.dumps({
            "id": str(_cuuid(identifier + name + "id")),
            "value": ARCHES[parent_uuid]
        }), lang="en")))

        graph.add((ARCHES[parent_uuid], SKOS.inScheme, URIRef(identifier)))

        if lst is not None:
            for child in lst:
                child_uuid = str(_cuuid(identifier + name + child))
                graph.add((ARCHES[child_uuid], RDF.type, SKOS.Concept))
                graph.add((ARCHES[child_uuid], SKOS.prefLabel, Literal(json.dumps({
                    "id": str(_cuuid(identifier + name + child + "id")),
                    "value": child
                }), lang="en")))

                graph.add((ARCHES[child_uuid], SKOS.inScheme, URIRef(identifier)))

                graph.add((ARCHES[parent_uuid], SKOS.narrower, ARCHES[child_uuid]))

        cgraph.add((collection_node[0], SKOS.member, ARCHES[parent_uuid]))
        cgraph.add((ARCHES[parent_uuid], RDF.type, SKOS.Concept))

        for object in graph.objects(subject=ARCHES[parent_uuid], predicate=SKOS.narrower):
            cgraph.add((ARCHES[parent_uuid], SKOS.member, object))
            cgraph.add((object, RDF.type, SKOS.Concept))

    return graph, cgraph

# This attempts to reduce the divergence in git diff - not much can be done for collections.xml sensibly.
def _reorder_output(concept_etree, collection_etree, orderings, nss, scheme_bearer):
    def _sort(rdf, label):
        # This sorts and appends to the end
        rdf[:] = sorted(
            rdf,
            key=lambda node: -(
                orderings[label].get(node.get(f"{{{nss[label]['rdf']}}}about")) or 0
            ),
            reverse=True
        )
        return rdf

    if scheme_bearer is not None:
        concept_etree_root = concept_etree.getroot()
        old_scheme = concept_etree_root.xpath(f"//skos:Concept[@rdf:about='{scheme_bearer}']/skos:inScheme", namespaces=nss["concept"])[0]
        old_scheme_bearer = old_scheme.getparent()
        if (scheme := concept_etree_root.xpath("//skos:Concept[./*/skos:ConceptScheme]/skos:inScheme", namespaces=nss["concept"])):
            print(scheme[0])
            scheme_bearer = scheme[0].getparent()
            old_scheme_bearer.append(scheme[0])
            scheme_bearer.append(old_scheme)
        concept_etree_root = ET.ElementTree(concept_etree_root)

    concept_etree = ET.ElementTree(_sort(concept_etree.getroot(), "concept"))
    ET.indent(concept_etree)
    ET.indent(collection_etree)
    collection_etree = ET.ElementTree(_sort(collection_etree.getroot(), "collection"))

    return concept_etree, collection_etree


if __name__ == "__main__":
    from csv import reader
    from lxml import etree as ET
    import argparse

    parser = argparse.ArgumentParser(
        prog="arches_graphql_client.concept",
        description="Utility for extending SKOS definitions",
    )
    parser.add_argument("concept_xml")
    parser.add_argument("collection_xml")
    parser.add_argument("-i", "--input", help="Input CSV with one column to add, or two columns if parent-child list", required=True)
    parser.add_argument("-c", "--collection", help="Label of parent collection", required=True)
    args = parser.parse_args()

    types: Mapping[str, Iterable[str] | None] = {}
    with open(args.input, newline="") as csvfile:
        for n, row in enumerate(reader(csvfile)):
            if len(row) == 1:
                types[row[0]] = None
            elif len(row) == 2:
                types[row[0]] = row[1]
            else:
                raise ValueError(f"Row {n}: rows must contain either 1 or 2 labels, comma-separated")

    orderings = {}
    nss = {}
    scheme_bearer = None
    for label, xml in (("concept", args.concept_xml), ("collection", args.collection_xml)):
        etree_root = ET.parse(xml).getroot()
        nss[label] = etree_root.nsmap
        if (scheme := etree_root.xpath("//skos:Concept[./*/skos:ConceptScheme]", namespaces=nss[label])):
            scheme_bearer = scheme[0].get(f"{{{nss[label]['rdf']}}}about")
        orderings[label] = {
            node: n for n, node in enumerate(etree_root.xpath("//@rdf:about", namespaces=nss[label]))
        }

    with open(args.concept_xml) as concept_xml_f, open(args.collection_xml) as collection_xml_f:
        concept_graph, collection_graph = extend_skos_consistently(
            concept_xml_f,
            collection_xml_f,
            args.collection,
            types
        )

    concept_etree = ET.ElementTree(
        ET.fromstring(concept_graph.serialize(format="pretty-xml").encode("UTF-8"))
    )
    collection_etree = ET.ElementTree(
        ET.fromstring(collection_graph.serialize(format="pretty-xml").encode("UTF-8"))
    )

    concept_etree, collection_etree = _reorder_output(concept_etree, collection_etree, orderings, nss, scheme_bearer)

    concept_etree.write(args.concept_xml)
    collection_etree.write(args.collection_xml)
