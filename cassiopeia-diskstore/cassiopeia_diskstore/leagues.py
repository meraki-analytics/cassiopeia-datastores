from typing import Type, TypeVar, MutableMapping, Any, Iterable

from datapipelines import DataSource, DataSink, PipelineContext, Query, validate_query

from cassiopeia.data import Platform, Region, Queue
from cassiopeia.dto.league import LeaguePositionsDto, LeaguesListDto, MasterLeagueListDto, ChallengerLeagueListDto, LeagueListDto
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

    # League positions

    _validate_get_league_positions_query = Query. \
        has("summoner.id").as_(int).also. \
        has("platform").as_(Platform)

    @get.register(LeaguePositionsDto)
    @validate_query(_validate_get_league_positions_query, convert_region_to_platform)
    def get_league_positions(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LeaguePositionsDto:
        key = "{clsname}.{platform}.{id}".format(clsname=LeaguePositionsDto.__name__,
                                                 platform=query["platform"].value,
                                                 id=query["summoner.id"])
        return LeaguePositionsDto(self._get(key))

    @put.register(LeaguePositionsDto)
    def put_league_positions(self, item: LeaguePositionsDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{id}".format(clsname=LeaguePositionsDto.__name__,
                                                 platform=platform,
                                                 id=item["summonerId"])
        self._put(key, item)

    # Leagues

    _validate_get_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("id").as_(str)

    @get.register(LeagueListDto)
    @validate_query(_validate_get_league_query, convert_region_to_platform)
    def get_leagues(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LeagueListDto:
        key = "{clsname}.{platform}.{id}".format(clsname=LeagueListDto.__name__,
                                                 platform=query["platform"].value,
                                                 id=query["id"])
        return LeagueListDto(self._get(key))

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

    # Master

    _validate_get_master_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("queue").as_(Queue)

    @get.register(MasterLeagueListDto)
    @validate_query(_validate_get_master_league_query, convert_region_to_platform)
    def get_mastery_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MasterLeagueListDto:
        key = "{clsname}.{platform}.{queue}".format(clsname=MasterLeagueListDto.__name__,
                                                    platform=query["platform"].value,
                                                    queue=query["queue"].value)
        return MasterLeagueListDto(self._get(key))

    @put.register(MasterLeagueListDto)
    def put_mastery_league(self, item: MasterLeagueListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        key = "{clsname}.{platform}.{queue}".format(clsname=MasterLeagueListDto.__name__,
                                                    platform=platform,
                                                    queue=item["queue"])
        self._put(key, item)
