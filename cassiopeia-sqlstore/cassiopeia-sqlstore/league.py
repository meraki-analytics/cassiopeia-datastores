from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, ForeignKeyConstraint, Numeric
from sqlalchemy.orm import foreign, remote

from cassiopeia.dto.league import LeagueListDto, LeaguePositionDto, LeaguePositionsDto
from cassiopeia.dto.common import DtoObject

from .common import metadata, SQLBaseObject, map_object

class LeagueMiniSeriesDto(DtoObject):
    pass

class SQLLeagueMiniSeries(SQLBaseObject):
    _dto_type = LeagueMiniSeriesDto
    _table = Table("league_miniseries", metadata,
                    Column("leagueId", String(36), primary_key=True),
                    Column("playerOrTeamId", Integer, primary_key=True),
                    Column("platformId", String(5), primary_key=True),
                    Column("target", Integer),
                    Column("wins", Integer),
                    Column("losses", Integer),
                    Column("progress", String(5)),
                    ForeignKeyConstraint(
                        ["leagueId", "playerOrTeamId", "platformId"],
                        ["league_position.leagueId", "league_position.playerOrTeamId", "league_position.platformId"]
                    ))

map_object(SQLLeagueMiniSeries)

class SQLLeaguePosition(SQLBaseObject):
    _dto_type = LeaguePositionDto
    _table = Table("league_position", metadata,
                    Column("leagueId", String(36), primary_key=True),
                    Column("playerOrTeamId", Integer, primary_key=True),
                    Column("platformId", String(5),primary_key=True),
                    Column("playerOrTeamName", String(30)),
                    Column("leaguePoints", Integer),
                    Column("rank", String(5)),
                    Column("wins", Integer),
                    Column("losses", Integer),
                    Column("veteran", Boolean),
                    Column("inactive", Boolean),
                    Column("freshBlood", Boolean),
                    Column("hotStreak", Boolean),
                    Column("lastUpdate", BigInteger),
                    ForeignKeyConstraint(
                        ["leagueId", "platformId"],
                        ["league.leagueId", "league.platformId"]
                    ))
    _relationships = {"miniSeries":(SQLLeagueMiniSeries,{"uselist":False})}
    def to_dto(self):
        dto = super().to_dto()
        if dto["miniSeries"] is None:
            dto.pop("miniSeries")
        return dto

map_object(SQLLeaguePosition)

class SQLLeague(SQLBaseObject):
    _dto_type = LeagueListDto
    _table = Table("league", metadata,
                    Column("leagueId", String(36), primary_key=True),
                    Column("platformId", String(5), primary_key=True),
                    Column("name", String(30)),
                    Column("tier", String(10)),
                    Column("queue", String(20)),
                    Column("lastUpdate", BigInteger))
    _relationships = {"entries":(SQLLeaguePosition,{"backref":"league"})}

    def updated(self):
        for e in self.entries:
            e.updated()
        super().updated()

map_object(SQLLeague)

class SQLLeaguePositions(SQLBaseObject):
    _dto_type = LeaguePositionsDto
    _table = Table("league_by_summoner", metadata,
                    Column("summonerId", Integer, primary_key=True),
                    Column("platformId", String(5), primary_key=True),
                    Column("lastUpdate", BigInteger))
    _relationships = {"positions":(SQLLeaguePosition,
                                    {"primaryjoin":
                                        (remote(_table.c.summonerId)==foreign(SQLLeaguePosition.playerOrTeamId)) &
                                        (remote(_table.c.platformId)==foreign(SQLLeaguePosition.platformId)),
                                    })
                    }

map_object(SQLLeaguePositions)
