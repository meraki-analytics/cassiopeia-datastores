from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean

from cassiopeia.dto.championmastery import ChampionMasteryDto

from .common import metadata, SQLBaseObject, map_object

class SQLChampionMastery(SQLBaseObject):
    _dto_type = ChampionMasteryDto
    _table = Table("championmastery", metadata,
                    Column("platformId", String(5), primary_key=True),
                    Column("summonerId", Integer, primary_key=True),
                    Column("championId", Integer, primary_key=True),
                    Column("chestGranted", Boolean),
                    Column("championLevel", Integer),
                    Column("championPoints", Integer),
                    Column("championPointsUntilNextLevel", Integer),
                    Column("championPointsSinceLastLevel", Integer),
                    Column("lastPlayTime", BigInteger),
                    Column("lastUpdate", BigInteger))

map_object(SQLChampionMastery)
