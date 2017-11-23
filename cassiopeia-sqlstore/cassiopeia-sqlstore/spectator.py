import copy
from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, ForeignKeyConstraint

from cassiopeia.data import Platform
from cassiopeia.dto.spectator import CurrentGameInfoDto
from cassiopeia.dto.common import DtoObject

from .common import metadata, SQLBaseObject, map_object


class CurrentGameParticipantDto(DtoObject):
    pass

class SQLCurrentGameParticipant(SQLBaseObject):
    _dto_type = CurrentGameParticipantDto
    _table = Table("current_game_participant",metadata,
                    Column("current_game_platformId", String(5), primary_key=True),
                    Column("current_game_gameId", BigInteger, primary_key=True),
                    Column("participantId", Integer, primary_key=True),
                    Column("teamId", Integer),
                    Column("spell1Id", Integer),
                    Column("spell2Id", Integer),
                    Column("championId", Integer),
                    Column("profileIconId", Integer),
                    Column("summonerName", String(30)),
                    Column("bot", Boolean),
                    Column("summonerId", Integer),
                    ForeignKeyConstraint(
                        ["current_game_platformId", "current_game_gameId"],
                        ["current_game.platformId", "current_game.gameId"]
                    ))

map_object(SQLCurrentGameParticipant)

class CurrentGameBanDto(DtoObject):
    pass

class SQLCurrentGameBan(SQLBaseObject):
    _dto_type = CurrentGameBanDto
    _table = Table("current_game_ban", metadata,
                    Column("current_game_platformId", String(5), primary_key=True),
                    Column("current_game_gameId", BigInteger, primary_key=True),
                    Column("pickTurn", Integer, primary_key=True),
                    Column("teamId", Integer),
                    Column("championId", Integer),
                    ForeignKeyConstraint(
                        ["current_game_platformId", "current_game_gameId"],
                        ["current_game.platformId", "current_game.gameId"]
                    ))

map_object(SQLCurrentGameBan)

class SQLCurrentGameInfo(SQLBaseObject):
    _dto_type = CurrentGameInfoDto
    _table = Table("current_game", metadata,
                    Column("gameId", BigInteger, primary_key=True),
                    Column("platformId", String(5), primary_key=True),
                    Column("gameStartTime", BigInteger),
                    Column("gameMode", String(10)),
                    Column("mapId",Integer),
                    Column("gameType", String(12)),
                    Column("gameQueueConfigId", Integer),
                    Column("gameLength", Integer),
                    Column("encryptionKey", String(32)),
                    Column("featured", Boolean),
                    Column("lastUpdate", BigInteger))
    _relationships = {"bannedChampions":(SQLCurrentGameBan,{}), "participants": (SQLCurrentGameParticipant,{})}

    def __init__(self,featured=False, **kwargs):
        i = 1
        for participant in kwargs["participants"]:
            participant["participantId"] = i
            i += 1
        kwargs["encryptionKey"] = kwargs["observers"]["encryptionKey"]
        kwargs["featured"] = featured
        super().__init__(**kwargs)

    def to_dto(self):
        dto = super().to_dto()
        dto["observers"] = {"encryptionKey":dto["encryptionKey"]}
        dto["region"] = Platform(dto.pop("platformId")).region.value
        return dto

map_object(SQLCurrentGameInfo)
