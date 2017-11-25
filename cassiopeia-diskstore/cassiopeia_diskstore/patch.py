from typing import Type, TypeVar, MutableMapping, Any, Iterable

from datapipelines import DataSource, DataSink, PipelineContext

from cassiopeia.dto.patch import PatchListDto
from .common import SimpleKVDiskService

T = TypeVar("T")


class PatchDiskService(SimpleKVDiskService):

    @DataSource.dispatch
    def get(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type: Type[T], query: MutableMapping[str, Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @DataSink.dispatch
    def put(self, type: Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @DataSink.dispatch
    def put_many(self, type: Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    @get.register(PatchListDto)
    def get_patches(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> PatchListDto:
        key = "{clsname}".format(clsname=PatchListDto.__name__)
        return PatchListDto(self._get(key))

    @put.register(PatchListDto)
    def put_patches(self, item: PatchListDto, context: PipelineContext = None) -> None:
        key = "{clsname}".format(clsname=PatchListDto.__name__)
        self._put(key, item)
