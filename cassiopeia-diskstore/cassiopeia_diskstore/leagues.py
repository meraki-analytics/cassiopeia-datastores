from typing import Type, TypeVar, MutableMapping, Any, Iterable

from datapipelines import DataSource, DataSink, PipelineContext, Query, validate_query

from cassiopeia.data import Platform, Region, Queue
from cassiopeia.dto.league import MasterLeagueListDto, GrandmasterLeagueListDto, ChallengerLeagueListDto
from cassiopeia.datastores.uniquekeys import convert_region_to_platform
from .common import SimpleKVDiskService

T = TypeVar("T")


class LeaguesDiskService(SimpleKVDiskService):
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

    # Challenger

    _validate_get_challenger_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("queue").as_(Queue)

    @get.register(ChallengerLeagueListDto)
    @validate_query(_validate_get_challenger_league_query, convert_region_to_platform)
    def get_challenger_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChallengerLeagueListDto:
        key = "{clsname}.{platform}.{queue}".format(clsname=ChallengerLeagueListDto.__name__,
                                                    platform=query["platform"].value,
                                                    queue=query["queue"].value)
        return ChallengerLeagueListDto(self._get(key))

    @put.register(ChallengerLeagueListDto)
    def put_challenger_league(self, item: ChallengerLeagueListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{queue}".format(clsname=ChallengerLeagueListDto.__name__,
                                                    platform=platform,
                                                    queue=item["queue"])
        self._put(key, item)

    # Grandmaster

    _validate_get_grandmaster_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("queue").as_(Queue)

    @get.register(GrandmasterLeagueListDto)
    @validate_query(_validate_get_grandmaster_league_query, convert_region_to_platform)
    def get_grandmaster_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> GrandmasterLeagueListDto:
        key = "{clsname}.{platform}.{queue}".format(clsname=GrandmasterLeagueListDto.__name__,
                                                    platform=query["platform"].value,
                                                    queue=query["queue"].value)
        return GrandmasterLeagueListDto(self._get(key))

    @put.register(GrandmasterLeagueListDto)
    def put_grandmaster_league(self, item: GrandmasterLeagueListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{queue}".format(clsname=GrandmasterLeagueListDto.__name__,
                                                    platform=platform,
                                                    queue=item["queue"])
        self._put(key, item)

    # Master

    _validate_get_master_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("queue").as_(Queue)

    @get.register(MasterLeagueListDto)
    @validate_query(_validate_get_master_league_query, convert_region_to_platform)
    def get_master_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MasterLeagueListDto:
        key = "{clsname}.{platform}.{queue}".format(clsname=MasterLeagueListDto.__name__,
                                                    platform=query["platform"].value,
                                                    queue=query["queue"].value)
        return MasterLeagueListDto(self._get(key))

    @put.register(MasterLeagueListDto)
    def put_master_league(self, item: MasterLeagueListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{queue}".format(clsname=MasterLeagueListDto.__name__,
                                                    platform=platform,
                                                    queue=item["queue"])
        self._put(key, item)
