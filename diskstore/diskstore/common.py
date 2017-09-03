import os
import pickle
from abc import abstractmethod
from typing import Mapping, Any, TypeVar, Iterable, Type
from simplekv.fs import FilesystemStore

from datapipelines import DataSource, DataSink, PipelineContext, NotFoundError

from cassiopeia.dto.common import DtoObject

T = TypeVar("T")


class SimpleKVDiskService(DataSource, DataSink):
    def __init__(self, path: str = None):
        if path is None:
            import tempfile
            path = tempfile.gettempdir()
            path = os.path.join(path, "simplekv_store")
        if not os.path.exists(path):
            os.mkdir(path)
        self._store = FilesystemStore(path)

    @abstractmethod
    def get(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @abstractmethod
    def get_many(self, type: Type[T], query: Mapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @abstractmethod
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @abstractmethod
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    def _get(self, key: str):
        try:
            data = pickle.loads(self._store.get(key))
        except KeyError:
            raise NotFoundError
        return data

    def _put(self, key: str, item: DtoObject):
        if key not in self._store:
            pickle_item = pickle.dumps(item)
            pickle_item = pickle_item
            self._store.put(key, pickle_item)
