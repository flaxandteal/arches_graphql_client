import uuid
from gql import gql

from .client import BaseClient
from .utils import camel, studly


class ConceptClient(BaseClient):
    __url_prefix__ = "concepts/"

    async def get_concept_details(self, name_or_id):
        get_terms_query = gql(
            "query ($concept: String) { getConceptDetails(nameOrId: $concept) }"
        )
        return (
            await self.client.execute_async(
                get_terms_query, variable_values={"concept": name_or_id}
            )
        )["getConceptDetails"]

    async def get_concept(self, name_or_id):
        get_terms_query = gql(
            "query ($concept: String) { getConcept(nameOrId: $concept) { id, label, nodetype } }"
        )
        return (
            await self.client.execute_async(
                get_terms_query, variable_values={"concept": name_or_id}
            )
        )["getConcept"]

    async def get_available_concepts(self):
        get_available_concepts_query = gql(
            """
            query {
                getAvailableConcepts
            }
        """
        )
        return sorted(
            (await self.client.execute_async(get_available_concepts_query))[
                "getAvailableConcepts"
            ]
        )

    async def get_term_list(self, concept):
        get_terms_query = gql(
            "query ($concept: String) { getTermList(conceptNameOrId: $concept) }"
        )
        return (
            await self.client.execute_async(
                get_terms_query, variable_values={"concept": concept}
            )
        )["getTerms"]

    async def get_terms(self, concept):
        get_terms_query = gql(
            "query ($concept: String) { getTerms(conceptNameOrId: $concept) { label, fullLabel, identifier } }"
        )
        return [
            term
            for term in (
                await self.client.execute_async(
                    get_terms_query, variable_values={"concept": concept}
                )
            )["getTerms"]
        ]

    async def add_term(self, concept, label_or_full_label):
        if isinstance(label_or_full_label, list):
            identifier = "》".join(label_or_full_label)
        else:
            identifier = label_or_full_label

        if isinstance(concept, str):
            try:
                concept = {"id": uuid.UUID(concept)}
            except ValueError:
                concept = {"label": concept}
        add_term_mut = gql(
            "mutation ($identifier: String, $concept: ConceptInput) { addTerm(identifier: $identifier, concept: $concept) { ok } }"
        )
        return (
            await self.client.execute_async(
                add_term_mut,
                variable_values={"identifier": identifier, "concept": concept},
            )
        )["addTerm"]

    async def replace_from_skos(self, file):
        replace_from_skos_mut = gql(
            "mutation ($file: Upload!) { replaceFromSkos(file: $file) { ok } }"
        )
        return (
            await self.client.execute_async(
                replace_from_skos_mut,
                variable_values={"file": file},
                upload_files=True
            )
        )["replaceFromSkos"]
