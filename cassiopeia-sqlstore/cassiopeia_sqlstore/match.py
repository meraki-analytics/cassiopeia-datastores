from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, ForeignKey, ForeignKeyConstraint, Numeric

from cassiopeia.dto.match import MatchDto
from cassiopeia.dto.common import DtoObject

from .common import metadata, SQLBaseObject, map_object, SQLConstant

class MatchParticipantTimelineDeltasDto(DtoObject):
    pass

class SQLMatchParticipantTimelineDeltas(SQLBaseObject):
    _dto_type = MatchParticipantTimelineDeltasDto
    _table = Table("match_participant_timeline_deltas", metadata,
                    Column("match_platformId", String(5), primary_key=True),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("match_participant_participantId", Integer, primary_key=True),
                    # The column needs to be defined explicitly because it is a primary key
                    Column("typeId", Integer, primary_key=True),
                    Column("0-10", Numeric),
                    Column("10-20", Numeric),
                    Column("20-30", Numeric),
                    Column("30-end", Numeric),
                    ForeignKeyConstraint(
                        ["match_platformId","match_gameId","match_participant_participantId"],
                        ["match_participant_timeline.match_platformId","match_participant_timeline.match_gameId","match_participant_timeline.match_participant_participantId"]))
    _constants = ["type"]
map_object(SQLMatchParticipantTimelineDeltas)

class MatchParticipantTimelineDto(DtoObject):
    pass

class SQLMatchParticipantTimeline(SQLBaseObject):
    _dto_type = MatchParticipantTimelineDto
    _table = Table("match_participant_timeline", metadata,
                    Column("match_platformId", String(5), primary_key=True),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("match_participant_participantId", Integer, primary_key=True),
                    Column("lane", String),
                    Column("role", String),
                    ForeignKeyConstraint(
                        ["match_platformId","match_gameId","match_participant_participantId"],
                        ["match_participant.match_platformId","match_participant.match_gameId","match_participant.participantId"]))
    _relationships = {"deltas":(SQLMatchParticipantTimelineDeltas,{})}

    def __init__(self, **kwargs):
        kwargs["deltas"] = [{"type":key, **value}
                            for key, value in kwargs.items()
                            if key.endswith("Deltas")]
        super().__init__(**kwargs)

    def to_dto(self):
        dto = super().to_dto()
        deltas = dto.pop("deltas")
        for delta in deltas:
            dto[delta["type"]] = {key:value for key, value in delta.items() if key != "type"}
        return dto

map_object(SQLMatchParticipantTimeline)

class MatchParticipantStatsDto(DtoObject):
    pass

class SQLMatchParticipantStats(SQLBaseObject):
    _dto_type = MatchParticipantStatsDto
    _table = Table("match_participant_stats", metadata,
                    Column("match_platformId", String(5), primary_key=True),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("match_participant_participantId", Integer, primary_key=True),
                    Column("physicalDamageDealt", Integer),
                    Column("magicDamageDealt", Integer),
                    Column("neutralMinionsKilledTeamJungle", Integer),
                    Column("totalPlayerScore", Integer),
                    Column("deaths", Integer),
                    Column("win", Boolean),
                    Column("neutralMinionsKilledEnemyJungle", Integer),
                    Column("altarsCaptured", Integer),
                    Column("largestCriticalStrike", Integer),
                    Column("totalDamageDealt", Integer),
                    Column("magicDamageDealtToChampions", Integer),
                    Column("visionWardsBoughtInGame", Integer),
                    Column("damageDealtToObjectives", Integer),
                    Column("largestKillingSpree", Integer),
                    Column("item1", Integer),
                    Column("quadraKills", Integer),
                    Column("teamObjective", Integer),
                    Column("totalTimeCrowdControlDealt", Integer),
                    Column("longestTimeSpentLiving", Integer),
                    Column("wardsKilled", Integer),
                    Column("firstTowerAssist", Boolean),
                    Column("firstTowerKill", Boolean),
                    Column("item2", Integer),
                    Column("item3", Integer),
                    Column("item0", Integer),
                    Column("firstBloodAssist", Boolean),
                    Column("visionScore", Integer),
                    Column("wardsPlaced", Integer),
                    Column("item4", Integer),
                    Column("item5", Integer),
                    Column("item6", Integer),
                    Column("turretKills", Integer),
                    Column("tripleKills", Integer),
                    Column("damageSelfMitigated", Integer),
                    Column("champLevel", Integer),
                    Column("nodeNeutralizeAssist", Integer),
                    Column("firstInhibitorKill", Boolean),
                    Column("goldEarned", Integer),
                    Column("magicalDamageTaken", Integer),
                    Column("kills", Integer),
                    Column("doubleKills", Integer),
                    Column("nodeCaptureAssist", Integer),
                    Column("trueDamageTaken", Integer),
                    Column("nodeNeutralize", Integer),
                    Column("firstInhibitorAssist", Boolean),
                    Column("assists", Integer),
                    Column("unrealKills", Integer),
                    Column("neutralMinionsKilled", Integer),
                    Column("objectivePlayerScore", Integer),
                    Column("combatPlayerScore", Integer),
                    Column("damageDealtToTurrets", Integer),
                    Column("altarsNeutralized", Integer),
                    Column("goldSpent", Integer),
                    Column("trueDamageDealt", Integer),
                    Column("trueDamageDealtToChampions", Integer),
                    Column("pentaKills", Integer),
                    Column("totalHeal", Integer),
                    Column("totalMinionsKilled", Integer),
                    Column("firstBloodKill", Boolean),
                    Column("nodeCapture", Integer),
                    Column("largestMultKill", Integer),
                    Column("sightWardsBoughtInGame", Integer),
                    Column("totalDamageDealtToChampions", Integer),
                    Column("totalUnitsHealed", Integer),
                    Column("inhibitorKills", Integer),
                    Column("totalScoreRank", Integer),
                    Column("totalDamageTaken", Integer),
                    Column("killingSprees", Integer),
                    Column("timeCCingOthers", Integer),
                    Column("physicalDamageTaken", Integer),
                    Column("perk0", Integer),
                    Column("perk0Var1", Integer),
                    Column("perk0Var2", Integer),
                    Column("perk0Var3", Integer),
                    Column("perk1", Integer),
                    Column("perk1Var1", Integer),
                    Column("perk1Var2", Integer),
                    Column("perk1Var3", Integer),
                    Column("perk2", Integer),
                    Column("perk2Var1", Integer),
                    Column("perk2Var2", Integer),
                    Column("perk2Var3", Integer),
                    Column("perk3", Integer),
                    Column("perk3Var1", Integer),
                    Column("perk3Var2", Integer),
                    Column("perk3Var3", Integer),
                    Column("perk4", Integer),
                    Column("perk4Var1", Integer),
                    Column("perk4Var2", Integer),
                    Column("perk4Var3", Integer),
                    Column("perk5", Integer),
                    Column("perk5Var1", Integer),
                    Column("perk5Var2", Integer),
                    Column("perk5Var3", Integer),
                    Column("perkPrimaryStyle", Integer),
                    Column("perkSubStyle", Integer),
                    ForeignKeyConstraint(
                        ["match_platformId","match_gameId","match_participant_participantId"],
                        ["match_participant.match_platformId","match_participant.match_gameId","match_participant.participantId"]))

map_object(SQLMatchParticipantStats)

class MatchParticipantDto(DtoObject):
    pass

class SQLMatchParticipant(SQLBaseObject):
    _dto_type = MatchParticipantDto
    _table = Table("match_participant", metadata,
                    Column("match_platformId", String(5), primary_key=True),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("participantId", Integer, primary_key=True),
                    Column("championId", Integer),
                    Column("teamId", Integer),
                    Column("spell1Id", Integer),
                    Column("spell2Id", Integer),
                    ForeignKeyConstraint(
                        ["match_platformId","match_gameId"],
                        ["match.platformId","match.gameId"]))
    _relationships = {
                        "stats":(SQLMatchParticipantStats,{"uselist":False}),
                        "timeline":(SQLMatchParticipantTimeline,{"uselist":False})
                    }

map_object(SQLMatchParticipant)

class MatchParticipantsIdentitiesDto(DtoObject):
    pass

class SQLMatchParticipantsIdentities(SQLBaseObject):
    _dto_type = MatchParticipantsIdentitiesDto
    _table = Table("match_participant_identities", metadata,
                    Column("p_currentPlatformId", String(5), primary_key=True),
                    Column("p_platformId", String(5)),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("participantId", Integer, primary_key=True),
                    Column("p_accountId", Integer),
                    Column("p_summonerName", String),
                    Column("p_summonerId", Integer),                    
                    Column("p_currentAccountId", Integer),
                    Column("p_profileIcon", Integer),
                    ForeignKeyConstraint(
                        ["p_currentPlatformId","match_gameId"],
                        ["match.platformId","match.gameId"]))
    def __init__(self, **kwargs):
        player = kwargs.pop("player")
        for key, value in player.items():
            kwargs["p_" + key] = value
        super().__init__(**kwargs)

    def to_dto(self):
        dto = super().to_dto()
        player = {}
        for key, value in list(dto.items()):
            if key.startswith("p_"):
                newkey = key[2:]
                player[newkey] = dto.pop(key)
        dto["player"] = player
        # Create match history Uri
        dto["player"]["matchHistoryUri"] = "/v1/stats/player_history/" + player["platformId"] + "/" + str(player["accountId"])
        return dto

map_object(SQLMatchParticipantsIdentities)

class MatchBanDto(DtoObject):
    pass

class SQLMatchBan(SQLBaseObject):
    _dto_type = MatchBanDto
    _table = Table("match_ban", metadata,
                    Column("match_platformId",String(5), primary_key=True),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("pickTurn", Integer, primary_key=True),
                    Column("championId", Integer),
                    Column("teamId", Integer),
                    ForeignKeyConstraint(
                        ["match_platformId","match_gameId","teamId"],
                        ["match_team.match_platformId","match_team.match_gameId","match_team.teamId"]))

map_object(SQLMatchBan)

class MachTeamDto(DtoObject):
    pass

class SQLMatchTeam(SQLBaseObject):
    _dto_type = MachTeamDto
    _table =  Table("match_team", metadata,
                    Column("match_platformId", String(5), primary_key=True),
                    Column("match_gameId", BigInteger, primary_key=True),
                    Column("teamId", Integer, primary_key=True),
                    Column("firstDragon", Boolean),
                    Column("firstInhibitor", Boolean),
                    Column("firstRiftHerald", Boolean),
                    Column("firstBaron", Boolean),
                    Column("firstTower", Boolean),
                    Column("firstBlood", Boolean),
                    Column("baronKills", Integer),
                    Column("riftHeraldKills", Integer),
                    Column("vilemawKills", Integer),
                    Column("inhibitorKills", Integer),
                    Column("towerKills", Integer),
                    Column("dragonKills", Integer),
                    Column("win", Boolean),
                    ForeignKeyConstraint(
                        ["match_platformId","match_gameId"],
                        ["match.platformId","match.gameId"]))
    _relationships = {"bans":(SQLMatchBan, {})}
    def __init__(self, **dwargs):
        dwargs["win"] = dwargs["win"] == "Win"
        super().__init__(**dwargs)

    def to_dto(self):
        dto = super().to_dto()
        if dto["win"]:
            dto["win"] = "Win"
        else:
            dto["win"] = "Fail"
        return dto

map_object(SQLMatchTeam)

class SQLMatch(SQLBaseObject):
    _dto_type = MatchDto
    _table =  Table("match",metadata,
                    Column("platformId", String(5), primary_key=True),
                    Column("gameId", BigInteger, primary_key = True),
                    Column("seasonId", Integer),
                    Column("queueId", Integer),
                    Column("gameVersion", String(23)),
                    Column("mapId",Integer),
                    Column("gameDuration", Integer),
                    Column("gameCreation", BigInteger),
                    Column("lastUpdate", BigInteger))
    _relationships = {
                        "teams": (SQLMatchTeam, {}),
                        "participants":(SQLMatchParticipant,{"lazy":"selectin"}),
                        "participantIdentities":(SQLMatchParticipantsIdentities,{})
                    }
    _constants = ["gameType", "gameMode"]


map_object(SQLMatch)
