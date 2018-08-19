from typing import Type, TypeVar, MutableMapping, Any, Iterable

from datapipelines import DataSource, DataSink, PipelineContext, Query, validate_query

from cassiopeia_championgg.dto import ChampionGGStatsListDto, ChampionGGStatsDto
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
        can_have("elo").with_default(lambda *args, **kwargs: "PLATINUM_DIAMOND_MASTER_CHALLENGER", supplies_type=str)

    @get.register(ChampionGGStatsListDto)
    @validate_query(_validate_get_gg_champion_list_query, convert_region_to_platform)
    def get_champion_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionGGStatsListDto:
        patch = query["patch"]
        elo = query["elo"]
        key = "{clsname}.{patch}.{elo}".format(clsname=ChampionGGStatsListDto.__name__,
                                               patch=patch,
                                               elo=elo)
        data = self._get(key)
        data["data"] = [ChampionGGStatsDto(champion) for champion in data["data"]]
        return ChampionGGStatsListDto(data)

    @put.register(ChampionGGStatsListDto)
    def put_champion_list(self, item: ChampionGGStatsListDto, context: PipelineContext = None) -> None:
        key = "{clsname}.{patch}.{elo}".format(clsname=ChampionGGStatsListDto.__name__,
                                               patch=item["patch"],
                                               elo=item["elo"])
        self._put(key, item)
