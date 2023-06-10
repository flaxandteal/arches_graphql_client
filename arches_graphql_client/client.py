from gql import gql, Client
import base64
import uuid
from gql.transport.aiohttp import AIOHTTPTransport
import json
import pandas as pd
from abc import ABCMeta, abstractmethod
from .config import get


class BaseClient(metaclass=ABCMeta):
    client = None
    root = None

    def __init__(self, root):
        self.root = root

    @property
    @abstractmethod
    def __url_prefix__(self):
        ...

    def connect(self, timeout=30, headers=None):
        auth_config = get("auth")
        _headers = {}
        if (client_id := auth_config.get("client_id")) is not None and (client_secret := auth_config.get("client_secret")) is not None:
            credential = "{0}:{1}".format(client_id, client_secret)
            cred = base64.b64encode(credential.encode("utf-8"))
            auth_headers = {"Authorization": "Basic " + cred.decode("utf-8")}
            _headers.update(auth_headers)

        if headers is not None:
            _headers.update(headers)

        transport = AIOHTTPTransport(
            url=f"{self.root}{self.__url_prefix__}",
            headers=_headers
        )
        self.client = Client(
            transport=transport,
            fetch_schema_from_transport=True,
            execute_timeout=timeout,
        )
