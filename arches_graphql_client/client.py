from gql import gql, Client
import uuid
from gql.transport.aiohttp import AIOHTTPTransport
import json
import pandas as pd
from abc import ABCMeta, abstractmethod


class BaseClient(metaclass=ABCMeta):
    client = None
    root = None

    def __init__(self, root):
        self.root = root

    @property
    @abstractmethod
    def __url_prefix__(self):
        ...

    def connect(self, timeout=30):
        transport = AIOHTTPTransport(url=f"{self.root}{self.__url_prefix__}")
        self.client = Client(
            transport=transport,
            fetch_schema_from_transport=True,
            execute_timeout=timeout,
        )
