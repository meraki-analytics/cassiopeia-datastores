import copy
import datetime

from typing import Type, TypeVar, Mapping, MutableMapping, Any, Iterable
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from datapipelines import DataSource, DataSink, PipelineContext, Query, validate_query,NotFoundError

from cassiopeia.data import Platform, Region

from cassiopeia.dto.common import DtoObject
from cassiopeia.dto.summoner import SummonerDto
from cassiopeia.dto.match import MatchDto, TimelineDto
from cassiopeia.dto.championmastery import ChampionMasteryListDto, ChampionMasteryDto
from cassiopeia.dto.champion import ChampionListDto, ChampionDto
from cassiopeia.dto.spectator import CurrentGameInfoDto, FeaturedGamesDto
from cassiopeia.dto.league import LeagueListDto, LeaguePositionsDto

from cassiopeia.datastores.uniquekeys import convert_region_to_platform

from .common import metadata, SQLBaseObject, sql_classes
from .summoner import SQLSummoner
from .match import SQLMatch
from .timeline import SQLTimeline
from .champion import SQLChampionStatus
from .championmastery import SQLChampionMastery
from .spectator import SQLCurrentGameInfo, SQLCurrentGameParticipant
from .league import SQLLeague, SQLLeaguePositions, SQLLeaguePosition

T = TypeVar("T")

default_expirations = {
    ChampionDto: datetime.timedelta(days=1),
    ChampionListDto: datetime.timedelta(days=1),
    ChampionMasteryDto: datetime.timedelta(days=7),
    ChampionMasteryListDto: datetime.timedelta(days=7),
    MatchDto: -1,
    TimelineDto: -1,
    SummonerDto: datetime.timedelta(days=1),
    CurrentGameInfoDto: datetime.timedelta(hours=0.5),
    LeagueListDto:datetime.timedelta(hours=6),
    LeaguePositionsDto: datetime.timedelta(hours=6)
}

class SQLStore(DataSource, DataSink):
    def __init__(self, connection_string, debug=False, expirations:Mapping[type, float] = None) -> None:
        self._expirations = dict(expirations) if expirations is not None else default_expirations
        for key, value in self._expirations.items():
            if isinstance(key, str):
                new_key = globals()[key]
                self._expirations[new_key] = self._expirations.pop(key)
                key = new_key
            if isinstance(value, datetime.timedelta):
                self._expirations[key] = value.seconds + 24 * 60 * 60 * value.days

        self._engine = create_engine(connection_string, echo=debug)
        metadata.bind = self._engine
        metadata.create_all()
        self._session = sessionmaker(bind=self._engine)()

    def expire(self, type: Any=None):
        for cls in sql_classes:
            if type is None or type is cls._dto_type:
                cls.expire(self._session, self._expirations)

    @DataSource.dispatch
    def get(self, type: Type[T], query:Mapping[str,Any], context: PipelineContext = None) -> T:
        pass

    @DataSource.dispatch
    def get_many(self, type:Type[T], query: Mapping[str,Any], context: PipelineContext = None) -> Iterable[T]:
        pass

    @DataSink.dispatch
    def put(self,type:Type[T], item: T, context: PipelineContext = None) -> None:
        pass

    @DataSink.dispatch
    def put_many(self, type:Type[T], items: Iterable[T], context: PipelineContext = None) -> None:
        pass

    def _one(self, query):
        try:
            result = query.one()
            if result.has_expired(self._expirations):
                self._session.delete(result)
                self._session.commit()
                raise NotFoundError
            return query.one()
        except (NoResultFound, MultipleResultsFound):
            raise NotFoundError

    def _first(self,query):
        result = query.first()
        if result is None:
            raise NotFoundError
        else:
            if result.has_expired(self._expirations):
                self._session.delete(result)
                self._session.commit()
                raise NotFoundError
            return value

    def _all(self, query):
        if query.count() > 0:
            results = [value.to_dto() for value in query.all()]
            for result in results:
                if result.has_expired(self._expirations):
                    results.delete()
                    self._session.commit()
                    raise NotFoundError
            return results
        else:
            raise NotFoundError

    def _put(self, item:SQLBaseObject):
        item.updated()
        self._session.merge(item)
        self._session.commit()

    def _put_many(self, items:Iterable[DtoObject], cls):
        for item in items:
            i = cls(**item)
            i.updated()
            self._session.merge(i)
        self._session.commit()

    _validate_get_summoner_query = Query. \
        has("id").as_(int). \
        or_("account.id").as_(int). \
        or_("name").as_(str).also. \
        has("platform").as_(Platform)

    @get.register(SummonerDto)
    @validate_query(_validate_get_summoner_query, convert_region_to_platform)
    def get_summoner(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> SummonerDto:
        platform_str = query["platform"].value
        if "accountId" in query:
            summoner = self._one(self._session.query(SQLSummoner) \
                                    .filter_by(platform=platform_str) \
                                    .filter_by(accountId=query["accountId"]))
        elif "id" in query:
            summoner = self._one(self._session.query(SQLSummoner) \
                                    .filter_by(platform=platform_str) \
                                    .filter_by(id=query["id"]))
        elif "name" in query:
            summoner = self._first(self._session.query(SQLSummoner) \
                                        .filter_by(platform=platform_str) \
                                        .filter_by(name=query["name"]))
        else:
            raise RuntimeError("Impossible!")
        return summoner.to_dto()

    @put.register(SummonerDto)
    def put_summoner(self, item:SummonerDto, context: PipelineContext = None) -> None:
        if not "platform" in item:
            item["platform"] = Region(item["region"]).platform.value
        self._put(SQLSummoner(**item))

    _validate_get_match_query = Query. \
        has("id").as_(int).also. \
        has("platform").as_(Platform)

    @get.register(MatchDto)
    @validate_query(_validate_get_match_query, convert_region_to_platform)
    def get_match(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MatchDto:
        platform_str = query["platform"].value
        match = self._one(self._session.query(SQLMatch).filter_by(platformId=platform_str).filter_by(gameId=query["id"]))
        return match.to_dto()

    @put.register(MatchDto)
    def put_match(self, item:MatchDto, context: PipelineContext = None) -> None:
        self._put(SQLMatch(**item))

    _validate_get_timeline_query = Query. \
       has("id").as_(int).also. \
       has("platform").as_(Platform)

    @get.register(TimelineDto)
    @validate_query(_validate_get_match_query, convert_region_to_platform)
    def get_timeline(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> TimelineDto:
       platform = query["platform"].value
       timeline = self._one(self._session.query(SQLTimeline).filter_by(platformId=platform) \
                                           .filter_by(matchId=query["id"]))
       return timeline.to_dto()

    @put.register(TimelineDto)
    def put_timeline(self, item: TimelineDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        item["platformId"] = platform
        self._put(SQLTimeline(**item))

    _validate_get_champion_mastery_query = Query. \
        has("platform").as_(Platform).also. \
        has("summoner.id").also. \
        has("champion.id").as_(int)


    @get.register(ChampionMasteryDto)
    @validate_query(_validate_get_champion_mastery_query, convert_region_to_platform)
    def get_champion_mastery(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionMasteryDto:
        champions_query = copy.deepcopy(query)
        champions_query.pop("champion.id")
        champions = self.get_champion_mastery_list(query=champions_query, context=context)
        def find_matching_attribute(list_of_dtos, attrname, attrvalue):
            for dto in list_of_dtos:
                if dto.get(attrname, None) == attrvalue:
                    return dto

        champion = find_matching_attribute(champions["masteries"], "championId", query["champion.id"])
        if champion is None:
            raise NotFoundError
        return ChampionMasteryDto(champion)

    _validate_get_champion_mastery_list_query = Query. \
        has("platform").as_(Platform).also. \
        has("summoner.id").as_(int)

    @get.register(ChampionMasteryListDto)
    @validate_query(_validate_get_champion_mastery_list_query, convert_region_to_platform)
    def get_champion_mastery_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionMasteryListDto:
        platform = query["platform"].value
        region = query["platform"].region.value
        summoner = query["summoner.id"]
        masteries = self._all(self._session.query(SQLChampionMastery).filter_by(platformId=platform).filter_by(summonerId=summoner))
        return ChampionMasteryListDto({"region":region,"summonerId":summoner,"masteries":masteries})

    @put.register(ChampionMasteryListDto)
    def put_champion_mastery_list(self, item: ChampionMasteryListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        summoner = item["summonerId"]
        for cm in item["masteries"]:
            cm["platformId"] = platform
            cm["summonerId"] = summoner
        self._put_many(item["masteries"], SQLChampionMastery)

    _validate_get_champion_status_list_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("freeToPlay").with_default(False)

    @get.register(ChampionListDto)
    @validate_query(_validate_get_champion_status_list_query, convert_region_to_platform)
    def get_champion_status_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionListDto:
        platform = query["platform"].value
        region = query["platform"].region.value
        freeToPlay = query["freeToPlay"]
        if freeToPlay:
            champions = self._all(self._session.query(SQLChampionStatus).filter_by(platform=platform).filter_by(freeToPlay=freeToPlay))
        else:
            champions = self._all(self._session.query(SQLChampionStatus).filter_by(platform=platform))
        for champ in champions:
            champ["region"] = region
        return ChampionListDto({"region":region, "freeToPlay":freeToPlay, "champions":champions})

    @put.register(ChampionListDto)
    def put_champion_status_list(self, item: ChampionListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        for champ in item["champions"]:
            champ["platform"] = platform
        self._put_many(item["champion"], SQLChampionStatus)

    _validate_get_current_game_query = Query. \
        has("platform").as_(Platform).also. \
        has("summoner.id").as_(int)

    @get.register(CurrentGameInfoDto)
    @validate_query(_validate_get_current_game_query, convert_region_to_platform)
    def get_current_game(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> CurrentGameInfoDto:
        platform = query["platform"].value
        summonerId = query["summoner.id"]
        match = self._one(self._session.query(SQLCurrentGameInfo).join(SQLCurrentGameInfo.participants) \
                                .filter(SQLCurrentGameParticipant.summonerId == summonerId))
        return match.to_dto()

    @put.register(CurrentGameInfoDto)
    def put_current_game_info(self, item: CurrentGameInfoDto, context: PipelineContext = None) -> None:
        self._put(SQLCurrentGameInfo(**item))

    _validate_get_featured_games_query = Query. \
        has("platform").as_(Platform)

    @get.register(FeaturedGamesDto)
    @validate_query(_validate_get_featured_games_query, convert_region_to_platform)
    def get_featured_games(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> FeaturedGamesDto:
        platform = query["platform"].value
        games = self._all(self._session.query(SQLCurrentGameInfo).filter(SQLCurrentGameInfo.platformId == platform) \
                                .filter(SQLCurrentGameInfo.featured == True))
        return FeaturedGamesDto({"clientRefreshInterval": 300, "gameList":games})

    @put.register(FeaturedGamesDto)
    def put_featured_games(self, item: FeaturedGamesDto, context: PipelineContext = None) -> None:
        for game in item["gameList"]:
            self._put(SQLCurrentGameInfo(featured=True, **game))

    @put.register(LeagueListDto)
    def put_league_list(self, item: LeagueListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        item["platformId"] = platform
        self._put(SQLLeague(**item))

    _validate_get_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("id").as_(str)

    @get.register(LeagueListDto)
    @validate_query(_validate_get_league_query, convert_region_to_platform)
    def get_leagues(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LeagueListDto:
        platform = query["platform"].value
        league = self._one(self._session.query(SQLLeague).filter_by(platformId=platform) \
                                .filter_by(leagueId=query["id"]))
        return league.to_dto()


    _validate_get_league_positions_query = Query. \
        has("summoner.id").as_(int).also. \
        has("platform").as_(Platform)

    @get.register(LeaguePositionsDto)
    @validate_query(_validate_get_league_positions_query, convert_region_to_platform)
    def get_league_positions(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LeaguePositionsDto:
        platform = query["platform"].value
        summonerId = query["summoner.id"]
        positions = self._one(self._session.query(SQLLeaguePositions).filter_by(platformId=platform) \
                                    .filter_by(summonerId=summonerId))
        for position in positions.positions:
            league = position.league
            position.leagueId = league.leagueId
            position.leagueName = league.name
            position.tier = league.tier
            position.queueType = league.queue
        dto = positions.to_dto()
        dto["region"] = Platform(dto["platformId"]).region.value
        return dto

    @put.register(LeaguePositionsDto)
    def put_league_positions(self, input: LeaguePositionsDto, context: PipelineContext = None) -> None:
        item = copy.deepcopy(input)
        platform = Region(item["region"]).platform.value
        item["platformId"] = platform
        for i in item["positions"]:
            i["platformId"] = platform
            if self._session.query(SQLLeague).filter_by(platformId=platform) \
                        .filter_by(leagueId=i["leagueId"]).count() <= 0:
                #The league does not exist yet. create it with lastUpdate 0
                map = {
                    "leagueId": i["leagueId"],
                    "platformId": platform,
                    "name": i["leagueName"],
                    "tier": i["tier"],
                    "queue": i["queueType"],
                    "lastUpdate": 0
                }
                self._session.add(SQLLeague(**map))
                self._session.commit()
            self._put(SQLLeaguePosition(**i))
        item.pop("positions")
        self._put(SQLLeaguePositions(**item))
