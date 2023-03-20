import pytest
from arches_graphql_client.utils import string_to_enum

@pytest.mark.parametrize("sym,exp", {
    "Grade B+": "GradeBPlus",
    "1&2": "_1And2",
    "Nothing": "Nothing",
    "Talking to a Person": "TalkingToAPerson",
    "a": "A",
    "A": "A",
    "Corvette (Non Sail)": "CorvetteNonSail",
}.items())
def test_string_to_enum(sym, exp):
    assert string_to_enum(sym) == exp
