from typing import Type, TypeVar, MutableMapping, Any, Iterable

from datapipelines import DataSource, DataSink, PipelineContext, Query, validate_query

from cassiopeia_championgg.dto import ChampionGGListDto, ChampionGGDto
from cassiopeia.datastores.uniquekeys import convert_region_to_platform
from .common import SimpleKVDiskService

T = TypeVar("T")


class ChampionGGDiskService(SimpleKVDiskService):

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

    _validate_get_gg_champion_list_query = Query. \
        has("patch").as_(str).also. \
        can_have("includedData").with_default(lambda *args, **kwargs: "kda,damage,minions,wards,overallPerformanceScore,goldEarned", supplies_type=str).also. \
        can_have("elo").with_default(lambda *args, **kwargs: "PLATINUM_DIAMOND_MASTER_CHALLENGER", supplies_type=str).also. \
        can_have("limit").with_default(lambda *args, **kwargs: 300, supplies_type=int)

    @get.register(ChampionGGListDto)
    @validate_query(_validate_get_gg_champion_list_query, convert_region_to_platform)
    def get_champion_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionGGListDto:
        patch = query["patch"]
        included_data = query["includedData"]
        elo = query["elo"]
        limit = query["limit"]
        key = "{clsname}.{patch}.{included_data}.{elo}.{limit}".format(clsname=ChampionGGListDto.__name__,
                                                                       patch=patch,
                                                                       included_data=included_data,
                                                                       elo=elo,
                                                                       limit=limit)
        data = self._get(key)
        data["data"] = [ChampionGGDto(champion) for champion in data["data"]]
        return ChampionGGListDto(data)

    @put.register(ChampionGGListDto)
    def put_champion_list(self, item: ChampionGGListDto, context: PipelineContext = None) -> None:
        key = "{clsname}.{patch}.{included_data}.{elo}.{limit}".format(clsname=ChampionGGListDto.__name__,
                                                                       patch=item["patch"],
                                                                       included_data=item["includedData"],
                                                                       elo=item["elo"],
                                                                       limit=item["limit"])
        self._put(key, item)
