import datetime

from typing import Type, TypeVar, Mapping, MutableMapping, Any, Iterable
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from datapipelines import DataSource, DataSink, PipelineContext, Query, validate_query,NotFoundError

from cassiopeia.data import Platform, Region, Queue, Tier

from cassiopeia.dto.common import DtoObject
from cassiopeia.dto.summoner import SummonerDto
from cassiopeia.dto.match import MatchDto, TimelineDto
from cassiopeia.dto.championmastery import ChampionMasteryListDto, ChampionMasteryDto
from cassiopeia.dto.champion import ChampionListDto, ChampionDto
from cassiopeia.dto.spectator import CurrentGameInfoDto, FeaturedGamesDto
from cassiopeia.dto.league import LeagueListDto, LeaguePositionsDto, ChallengerLeagueListDto, MasterLeagueListDto
from cassiopeia.dto.status import ShardStatusDto

from cassiopeia.datastores.uniquekeys import convert_region_to_platform

from .common import metadata, SQLBaseObject, sql_classes, Constant
from .summoner import SQLSummoner
from .match import SQLMatch
from .timeline import SQLTimeline
from .champion import SQLChampionStatus
from .championmastery import SQLChampionMastery
from .spectator import SQLCurrentGameInfo, SQLCurrentGameParticipant
from .league import SQLLeague, SQLLeaguePosition, SQLLeaguePositions
from .status import SQLShardStatus

T = TypeVar("T")

'''
Note: Because of the implementation details, some Dtos share the same expiration
FeaturedGamesDto shares expiration of CurrentGameInfoDto
ChallengerLeagueListDto and MasterLeagueListDto share expiration of LeagueListDto
ChampionMasteryListDto shares expiration of ChampionMasteryDto
ChampionListDto shares expiration of ChampionDto
'''
default_expirations = {
    ChampionDto: datetime.timedelta(days=1),
    ChampionMasteryDto: datetime.timedelta(days=7),
    MatchDto: -1,
    TimelineDto: -1,
    SummonerDto: datetime.timedelta(days=1),
    CurrentGameInfoDto: datetime.timedelta(hours=0.5),
    LeagueListDto:datetime.timedelta(hours=6),
    LeaguePositionsDto: datetime.timedelta(hours=6),
    ShardStatusDto: datetime.timedelta(hours=1),
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

        # Create database connection
        self._engine = create_engine(connection_string, echo=debug)
        metadata.bind = self._engine
        metadata.create_all()
        self._session_factory = sessionmaker(bind=self._engine)
        self._session = scoped_session(self._session_factory)
        Constant._session = self._session

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

    def dbconnect(func):
        def inner(*args, **kwargs):
            session = args[0]._session()
            try:
                func(*args, **kwargs)
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()
        return inner
    
    def _one(self, query):
        """Gets one row from the query. Raises NotFoundError if there isn't a row or if there are multiple rows"""
        try:
            result = query.one()           
            if result.has_expired(self._expirations):
                raise NotFoundError            
            return result            
        except (NoResultFound, MultipleResultsFound):
            raise NotFoundError

    def _first(self,query):
        """Gets the first row of the query. Raises NotFoundError if there isn't a row"""
        result = query.first()
        if result is None:
            raise NotFoundError
        else:
            if result.has_expired(self._expirations):
                raise NotFoundError
            return result

    def _all(self, query):
        """Gets all rows of the query. Raises a NotFoundError if there are 0 rows"""
        if query.count() > 0:            
            results = query.all()            
            for result in results:
                if result.has_expired(self._expirations):
                    raise NotFoundError
            return results
        else:
            raise NotFoundError

    @dbconnect
    def _put(self, item:SQLBaseObject):
        """Puts a item into the database. Updates lastUpdate column"""
        if item._dto_type in self._expirations and self._expirations[item._dto_type] == 0:
            # The expiration time has been set to 0 -> shoud not be cached
            return
        item.updated()
        self._session().merge(item)

    @dbconnect
    def _put_many(self, items:Iterable[DtoObject], cls):
        """Puts many items into the database. Updates lastUpdate column for each of them"""
        if cls._dto_type in self._expirations and self._expirations[cls._dto_type] == 0:
            # The expiration time has been set to 0 -> shoud not be cached
            return
        session = self._session
        for item in items:
            item = cls(**item)
            item.updated()
            session.merge(item)



    ####################
    # Summoner Endpoint#
    ####################

    _validate_get_summoner_query = Query. \
        has("id").as_(int). \
        or_("account.id").as_(int). \
        or_("name").as_(str).also. \
        has("platform").as_(Platform)

    @dbconnect
    @get.register(SummonerDto)
    @validate_query(_validate_get_summoner_query, convert_region_to_platform)
    def get_summoner(self, query: MutableMapping[str, Any],context: PipelineContext) -> SummonerDto:
        session = self._session()
        platform_str = query["platform"].value
        if "account.id" in query:
            summoner = self._one(session.query(SQLSummoner) \
                                    .filter_by(platform=platform_str) \
                                    .filter_by(accountId=query["account.id"]))
        elif "id" in query:
            summoner = self._one(session.query(SQLSummoner) \
                                    .filter_by(platform=platform_str) \
                                    .filter_by(id=query["id"]))
        elif "name" in query:
            summoner = self._first(session.query(SQLSummoner) \
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



    ##################
    # Match Endpoint #
    ##################

    # Match

    _validate_get_match_query = Query. \
        has("id").as_(int).also. \
        has("platform").as_(Platform)

    @dbconnect
    @get.register(MatchDto)
    @validate_query(_validate_get_match_query, convert_region_to_platform)
    def get_match(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MatchDto:
        platform_str = query["platform"].value
        match = self._one(self._session().query(SQLMatch) \
                            .filter_by(platformId=platform_str) \
                            .filter_by(gameId=query["id"]))
        return match.to_dto()

    @put.register(MatchDto)
    def put_match(self, item:MatchDto, context: PipelineContext = None) -> None:
        self._put(SQLMatch(**item))

    # Timeline

    _validate_get_timeline_query = Query. \
       has("id").as_(int).also. \
       has("platform").as_(Platform)

    @dbconnect
    @get.register(TimelineDto)
    @validate_query(_validate_get_match_query, convert_region_to_platform)
    def get_timeline(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> TimelineDto:
       platform = query["platform"].value
       timeline = self._one(self._session().query(SQLTimeline) \
                                .filter_by(platformId=platform) \
                                .filter_by(matchId=query["id"]))
       return timeline.to_dto()

    @put.register(TimelineDto)
    def put_timeline(self, item: TimelineDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        item["platformId"] = platform
        self._put(SQLTimeline(**item))



    #############################
    # Champion Mastery Endpoint #
    #############################

    # Champion Mastery List

    _validate_get_champion_mastery_list_query = Query. \
        has("platform").as_(Platform).also. \
        has("summoner.id").as_(int)

    @dbconnect
    @get.register(ChampionMasteryListDto)
    @validate_query(_validate_get_champion_mastery_list_query, convert_region_to_platform)
    def get_champion_mastery_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionMasteryListDto:
        platform = query["platform"].value
        region = query["platform"].region.value
        summoner = query["summoner.id"]
        masteries = self._all(self._session().query(SQLChampionMastery) \
                                .filter_by(platformId=platform) \
                                .filter_by(summonerId=summoner))
        return ChampionMasteryListDto({"region":region,"summonerId":summoner,"masteries":[mastery.to_dto for mastery in masteries]})

    @put.register(ChampionMasteryListDto)
    def put_champion_mastery_list(self, item: ChampionMasteryListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        summoner = item["summonerId"]
        for cm in item["masteries"]:
            cm["platformId"] = platform
            cm["summonerId"] = summoner
        self._put_many(item["masteries"], SQLChampionMastery)



    #####################
    # Champion Endpoint #
    #####################

    _validate_get_champion_status_list_query = Query. \
        has("platform").as_(Platform).also. \
        can_have("freeToPlay").with_default(False)

    @dbconnect
    @get.register(ChampionListDto)
    @validate_query(_validate_get_champion_status_list_query, convert_region_to_platform)
    def get_champion_status_list(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChampionListDto:
        platform = query["platform"].value
        region = query["platform"].region.value
        freeToPlay = query["freeToPlay"]
        if freeToPlay:
            champions = self._all(self._session().query(SQLChampionStatus) \
                                    .filter_by(platform=platform) \
                                    .filter_by(freeToPlay=freeToPlay))
        else:
            champions = self._all(self._session().query(SQLChampionStatus) \
                                    .filter_by(platform=platform))
        champions = [champ.to_dto() for champ in champions]
        for champ in champions:
            champ["region"] = region
        return ChampionListDto({"region":region, "freeToPlay":freeToPlay, "champions":champions})

    @put.register(ChampionListDto)
    def put_champion_status_list(self, item: ChampionListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        for champ in item["champions"]:
            champ["platform"] = platform
        self._put_many(item["champions"], SQLChampionStatus)



    ######################
    # Spectator Endpoint #
    ######################

    # Curremt Game

    _validate_get_current_game_query = Query. \
        has("platform").as_(Platform).also. \
        has("summoner.id").as_(int)

    @dbconnect
    @get.register(CurrentGameInfoDto)
    @validate_query(_validate_get_current_game_query, convert_region_to_platform)
    def get_current_game(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> CurrentGameInfoDto:
        summonerId = query["summoner.id"]
        match = self._one(self._session().query(SQLCurrentGameInfo) \
                                .join(SQLCurrentGameInfo.participants) \
                                .filter(SQLCurrentGameParticipant.summonerId == summonerId))
        return match.to_dto()

    @put.register(CurrentGameInfoDto)
    def put_current_game_info(self, item: CurrentGameInfoDto, context: PipelineContext = None) -> None:
        self._put(SQLCurrentGameInfo(**item))


    # Featured Games

    _validate_get_featured_games_query = Query. \
        has("platform").as_(Platform)

    @dbconnect
    @get.register(FeaturedGamesDto)
    @validate_query(_validate_get_featured_games_query, convert_region_to_platform)
    def get_featured_games(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> FeaturedGamesDto:
        platform = query["platform"].value
        games = self._all(self._session().query(SQLCurrentGameInfo) \
                                .filter_by(platformId=platform) \
                                .filter_by(featured=True))
        return FeaturedGamesDto({"clientRefreshInterval": 300, "gameList":[game.to_dto() for game in games], "region": query["platform"].region.value})

    @put.register(FeaturedGamesDto)
    def put_featured_games(self, item: FeaturedGamesDto, context: PipelineContext = None) -> None:
        for game in item["gameList"]:
            self._put(SQLCurrentGameInfo(featured=True, **game))



    ###################
    # League Endpoint #
    ###################


    # Insert league
    @put.register(LeagueListDto)
    @put.register(ChallengerLeagueListDto)
    @put.register(MasterLeagueListDto)
    @dbconnect
    def put_league(self, item: LeagueListDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        item["platformId"] = platform
        # Get league to update it later if it exists
        league = self._session().query(SQLLeague) \
                    .filter_by(platformId=platform) \
                    .filter_by(leagueId=item["leagueId"]).first()
        if not league:
            # League isn't present yet. Just insert it
            self._put(SQLLeague(**item))
            return
        
        # Map new entries by summoner id for easier lookup later on
        map_by_id = {int(value["playerOrTeamId"]):{**value,"playerOrTeamId":int(value["playerOrTeamId"])} for value in item["entries"]}
        
        # Iterate over existing entries
        for i in reversed(range(len(league.entries))):
            entry = league.entries[i]
            if entry.playerOrTeamId in map_by_id:
                # Entry is still present, just update it
                entry.__init__(**map_by_id[entry.playerOrTeamId])
                # Delete it from the map, so it doesn't get added twice later
                del map_by_id[entry.playerOrTeamId]
            else:
                # Entry isn't present anymore, remove it
                del league.entries[i]
        # append remaining entries
        for value in map_by_id.values():
            league.entries.append(SQLLeaguePosition(**value))

        league.updated()        
        self._session().merge(league)

    # Get league by id, challenger or master

    _validate_get_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("id").as_(str)

    @dbconnect
    @get.register(LeagueListDto)
    @validate_query(_validate_get_league_query, convert_region_to_platform)
    def get_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LeagueListDto:
        platform = query["platform"].value
        league = self._one(self._session().query(SQLLeague) \
                                .filter_by(platformId=platform) \
                                .filter_by(leagueId=query["id"]))
        return league.to_dto()

    _validate_get_challenger_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("queue").as_(Queue)

    @dbconnect
    @get.register(ChallengerLeagueListDto)
    @validate_query(_validate_get_challenger_league_query, convert_region_to_platform)
    def get_challenger_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ChallengerLeagueListDto:
        platform = query["platform"].value
        queue = query["queue"].value
        league = self._one(self._session().query(SQLLeague) \
                                .filter_by(platformId=platform) \
                                .filter_by(queue=queue) \
                                .filter_by(tier=Tier.challenger.value))
        return ChallengerLeagueListDto(**league.to_dto())

    _validate_get_master_league_query = Query. \
        has("platform").as_(Platform).also. \
        has("queue").as_(Queue)

    @dbconnect
    @get.register(MasterLeagueListDto)
    @validate_query(_validate_get_master_league_query, convert_region_to_platform)
    def get_master_league(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> MasterLeagueListDto:
        platform = query["platform"].value
        queue = query["queue"].value
        league = self._one(self._session().query(SQLLeague) \
                                .filter_by(platformId=platform) \
                                .filter_by(queue=queue) \
                                .filter_by(tier=Tier.master.value))
        return MasterLeagueListDto(**league.to_dto())



    # League Positions by summoner
    _validate_get_league_positions_query = Query. \
        has("summoner.id").as_(int).also. \
        has("platform").as_(Platform)

    @dbconnect
    @get.register(LeaguePositionsDto)
    @validate_query(_validate_get_league_positions_query, convert_region_to_platform)
    def get_league_positions(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> LeaguePositionsDto:
        platform = query["platform"].value
        summonerId = query["summoner.id"]
        positions = self._one(self._session().query(SQLLeaguePositions) \
                                    .filter_by(platformId=platform) \
                                    .filter_by(summonerId=summonerId))
        dto = positions.to_dto()
        dto["region"] = Platform(dto["platformId"]).region.value
        return dto

    @dbconnect
    @put.register(LeaguePositionsDto)
    def put_league_positions(self, item: LeaguePositionsDto, context: PipelineContext = None) -> None:
        platform = Region(item["region"]).platform.value
        item["platformId"] = platform     
        old_positions = []
        query = self._session().query(SQLLeaguePosition) \
                    .filter_by(platformId=platform) \
                    .filter_by(playerOrTeamId=item["summonerId"])
        if query.count() > 0:
            # There are already positions stored for that summoner
            old_positions = query.all()

        map_by_id = {position["leagueId"]:position for position in item["positions"]}

        for i in range(len(old_positions)):
            if old_positions[i].leagueId in map_by_id:
                # The given position is already present. update it
                old_positions[i].__init__(**map_by_id[old_positions[i].leagueId])
                # Remove from map to prevent inserting it twice later on
                del map_by_id[old_positions[i].leagueId]
                self._session().merge(old_positions[i])
            else:
                # The given position is no longer present. remove it
                old_positions[i].delete()

        for pos in map_by_id.values():
            # Create league, so it does get created in the database if it doesn't exist
            league = SQLLeague(
                platformId = platform,
                leagueId   = pos["leagueId"],
                name       = pos["leagueName"],
                tier       = pos["tier"],
                queue      = pos["queueType"]
            )
            position = SQLLeaguePosition(**pos,league=league)
            # This will not call SQlLeague.updated() to make sure we don't mess up the expiration of the hole league
            position.updated()
            self._session().merge(position)

        self._put(SQLLeaguePositions(summonerId=item["summonerId"],platformId=platform, positions=[]))



    #######################
    # LoL-Status Endpoint #
    #######################

    _validate_get_status_query = Query. \
        has("platform").as_(Platform)

    @dbconnect
    @get.register(ShardStatusDto)
    @validate_query(_validate_get_status_query, convert_region_to_platform)
    def get_status(self, query: MutableMapping[str, Any], context: PipelineContext = None) -> ShardStatusDto:
        status = self._one(self._session().query(SQLShardStatus) \
                                .filter_by(slug=query["platform"].region.value.lower()))
        return status.to_dto()

    @dbconnect
    @put.register(ShardStatusDto)
    def put_status(self, item: ShardStatusDto, context: PipelineContext = None) -> None:    
        try:
            # Try to get stored shard status to update it
            shard = self._session().query(SQLShardStatus) \
                        .filter_by(slug=item["slug"]).one()
            shard.__init__(**item)
            self._session().merge(shard)
        except:
            # No shard status stored. Just put a new one
            self._put(SQLShardStatus(**item))
            pass
