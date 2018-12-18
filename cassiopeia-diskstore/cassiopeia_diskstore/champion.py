from typing import Type, TypeVar, MutableMapping, Any, Iterable
import copy

from datapipelines import DataSource, DataSink, PipelineContext, Query, NotFoundError, validate_query

from cassiopeia.data import Platform, Region
from cassiopeia.dto.champion import ChampionRotationDto
from cassiopeia.datastores.uniquekeys import convert_region_to_platform
from .common import SimpleKVDiskService

T = TypeVar("T")


class ChampionDiskService(SimpleKVDiskService):
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

    #############
    # Champions #
    #############
    _validate_get_champion_status_list_query = Query. \
        has("platform").as_(Platform)

    @get.register(ChampionRotationDto)
    @validate_query(_validate_get_champion_status_list_query, convert_region_to_platform)
    def get_champion_status_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionRotationDto:
        platform = query["platform"].value
        key = "{clsname}.{platform}".format(clsname="ChampionRotationDto",
                                                           platform=platform)
        return ChampionRotationDto(self._get(key))

    @put.register(ChampionRotationDto)
    def put_champion_status_list(self, item: ChampionRotationDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}".format(clsname="ChampionRotationDto",
                                                           platform=platform)
        self._put(key, item)
