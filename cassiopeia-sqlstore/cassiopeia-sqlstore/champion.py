from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean

from cassiopeia.dto.champion import ChampionDto

from .common import metadata, SQLBaseObject, map_object

class SQLChampionStatus(SQLBaseObject):
    _dto_type = ChampionDto
    _table = Table("champion", metadata,
                    Column("id", Integer, primary_key=True),
                    Column("platform", String(5), primary_key=True),
                    Column("active", Boolean),
                    Column("botEnabled", Boolean),
                    Column("botMmEnabled", Boolean),
                    Column("freeToPlay", Boolean),
                    Column("rankedPlayEnabled", Boolean),
                    Column("lastUpdate", BigInteger))

map_object(SQLChampionStatus)
