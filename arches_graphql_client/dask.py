import dask
import logging
from dask.distributed import Client
import dask.distributed

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 300

def execute(ENDPOINT, values, model, model_name, headers=None, do_index=True):
    import arches_graphql_client
    import asyncio

    client = arches_graphql_client.ResourceClient(ENDPOINT, model, model_name)
    client.connect(timeout=DEFAULT_TIMEOUT, headers=headers)
    results = asyncio.run((client.bulk_create)(values, do_index=do_index))
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
    do_index=True,
    detach=False,
    model="monument",
    model_name="monument_name",
    headers=None
):
    results = []

    with Client(cluster) as dask_client:
        dask_client.submit(_install).result()
        for n, group in enumerate(groups):
            logger.info(f"{n} / {len(groups)}")
            fut = dask_client.submit(execute, ENDPOINT, group, model, model_name, headers, do_index)
            if detach:
                dask.distributed.fire_and_forget(fut)
            else:
                results += fut.result()

    return results
