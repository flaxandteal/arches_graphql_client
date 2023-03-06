import dask
from dask.distributed import Client


def execute(ENDPOINT, values, model, model_name):
    import arches_graphql_client
    import asyncio

    client = arches_graphql_client.ResourceClient(ENDPOINT, model, model_name)
    client.connect(timeout=300)
    results = asyncio.run((client.bulk_create)(values))
    return results


def _install():
    import os

    os.system(
        "pip install -q lz4 arches_graphql_client graphene gql aiohttp dateparser python-slugify gql openpyxl pandas tqdm aiohttp"
    )


def install():
    dask.delayed(_install)().compute()


def bulk_create_from_groups(
    groups,
    cluster,
    ENDPOINT,
    detach=False,
    model="monument",
    model_name="monument_name",
):
    results = []

    with Client(cluster) as dask_client:
        dask_client.submit(_install).result()
        for n, group in enumerate(groups):
            print("{n} / {len(groups)}")
            fut = dask_client.submit(execute, ENDPOINT, group, model, model_name)
            if detach:
                dask.distributed.fire_and_forget(fut)
            else:
                results += fut.result()

    return results
