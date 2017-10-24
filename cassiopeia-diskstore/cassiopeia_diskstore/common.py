import os
import pickle
import datetime
from abc import abstractmethod
from typing import Mapping, Any, TypeVar, Iterable, Type
import simplekv, simplekv.fs

from datapipelines import DataSource, DataSink, PipelineContext, NotFoundError

from cassiopeia.dto.common import DtoObject
from cassiopeia.dto.champion import ChampionDto as ChampionStatusDto, ChampionListDto as ChampionStatusListDto
from cassiopeia.dto.championmastery import ChampionMasteryDto, ChampionMasteryListDto
from cassiopeia.dto.league import LeaguePositionsDto, LeagueListDto, MasterLeagueListDto, ChallengerLeagueListDto
from cassiopeia.dto.staticdata import ChampionDto, ChampionListDto, MasteryDto, MasteryListDto, RuneDto, RuneListDto, ItemDto, ItemListDto, SummonerSpellDto, SummonerSpellListDto, MapDto, MapListDto, RealmDto, ProfileIconDataDto, ProfileIconDetailsDto, LanguagesDto, LanguageStringsDto, VersionListDto
from cassiopeia.dto.match import MatchDto, TimelineDto
from cassiopeia.dto.masterypage import MasteryPageDto, MasteryPagesDto
from cassiopeia.dto.runepage import RunePageDto, RunePagesDto
from cassiopeia.dto.summoner import SummonerDto
from cassiopeia.dto.status import ShardStatusDto
from cassiopeia.dto.spectator import CurrentGameInfoDto, FeaturedGamesDto

T = TypeVar("T")


default_expirations = {
    RealmDto: datetime.timedelta(hours=6),
    VersionListDto: datetime.timedelta(hours=6),
    ChampionDto: -1,
    ChampionListDto: -1,
    MasteryDto: -1,
    MasteryListDto: -1,
    RuneDto: -1,
    RuneListDto: -1,
    ItemDto: -1,
    ItemListDto: -1,
    SummonerSpellDto: -1,
    SummonerSpellListDto: -1,
    MapDto: -1,
    MapListDto: -1,
    ProfileIconDetailsDto: -1,
    ProfileIconDataDto: -1,
    LanguagesDto: -1,
    LanguageStringsDto: -1,
    ChampionStatusDto: datetime.timedelta(days=1),
    ChampionStatusListDto: datetime.timedelta(days=1),
    ChampionMasteryDto: datetime.timedelta(days=7),
    ChampionMasteryListDto: datetime.timedelta(days=7),
    LeaguePositionsDto: datetime.timedelta(hours=6),
    LeagueListDto: datetime.timedelta(hours=6),
    ChallengerLeagueListDto: datetime.timedelta(hours=6),
    MasterLeagueListDto: datetime.timedelta(hours=6),
    MatchDto: -1,
    TimelineDto: -1,
    MasteryPageDto: datetime.timedelta(days=1),
    MasteryPagesDto: datetime.timedelta(days=1),
    RunePageDto: datetime.timedelta(days=1),
    RunePagesDto: datetime.timedelta(days=1),
    SummonerDto: datetime.timedelta(days=1),
    ShardStatusDto: datetime.timedelta(hours=1),
    CurrentGameInfoDto: datetime.timedelta(hours=0.5),
    FeaturedGamesDto: datetime.timedelta(hours=0.5),
}


class SimpleKVDiskService(DataSource, DataSink):
    def __init__(self, path: str = None, expirations: Mapping[type, float] = None):
        if path is None:
            import tempfile
            path = tempfile.gettempdir()
            path = os.path.join(path, "simplekv_store")
        if not os.path.exists(path):
            os.mkdir(path)
        self._store = simplekv.fs.FilesystemStore(path)
        self._expirations = dict(expirations) if expirations is not None else default_expirations
        for key, value in self._expirations.items():
            if isinstance(key, str):
                new_key = globals()[key]
                self._expirations[new_key] = self._expirations.pop(key)
                key = new_key
            if value == -1:
                self._expirations[key] = simplekv.FOREVER
            elif isinstance(value, datetime.timedelta):
                self._expirations[key] = value.seconds + 24 * 60 * 60 * value.days

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
            data, timeout, entered = pickle.loads(self._store.get(key))
            now = datetime.datetime.now().timestamp()
            if timeout != "forever" and now > entered + timeout:
                self._store.delete(key)
                raise NotFoundError
        except KeyError:
            raise NotFoundError
        return data

    def _put(self, key: str, item: DtoObject):
        expire_seconds = self._expirations.get(item.__class__, default_expirations[item.__class__])

        if expire_seconds != 0 and key not in self._store:
            item = (item, expire_seconds, datetime.datetime.now().timestamp())
            pickle_item = pickle.dumps(item)
            pickle_item = pickle_item
            self._store.put(key, pickle_item)

    def clear(self, type: Type[T] = None):
        if type is None:
            for key in self._store:
                self._store.delete(key)
        else:
            typename = type.__name__
            for key in self._store:
                if key.startswith(typename):
                    self._store.delete(key)

    def expire(self, type: Any = None):
        if type is not None:
            for key in self._store.keys():
                self._get(key)
        else:
            typename = type.__name__
            for key in self._store.keys():
                if key.startswith(typename):
                    self._get(key)
