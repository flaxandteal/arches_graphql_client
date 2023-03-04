from arches_graphql_client import ResourceClient

def test_can_create_resource_client():
    asset_cl = ResourceClient("http://example.org", "Asset", "label_name")
