from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, ForeignKeyConstraint, Numeric

from decimal import Decimal

from cassiopeia.dto.match import MatchDto
from cassiopeia.dto.common import DtoObject

from .common import metadata, SQLBaseObject, map_object


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
                   Column("0-10", Numeric(precision=7, scale=3)),
                   Column("10-20", Numeric(precision=7, scale=3)),
                   Column("20-30", Numeric(precision=7, scale=3)),
                   Column("30-end", Numeric(precision=7, scale=3)),
                   ForeignKeyConstraint(
                       ["match_platformId", "match_gameId", "match_participant_participantId"],
                       ["match_participant_timeline.match_platformId", "match_participant_timeline.match_gameId",
                        "match_participant_timeline.match_participant_participantId"]))
    _constants = ["type"]

    def to_dto(self):
        dto = super().to_dto()
        for key, value in dto.items():
            if type(value) is Decimal:
                dto[key] = float(value)
        return dto


map_object(SQLMatchParticipantTimelineDeltas)


class MatchParticipantTimelineDto(DtoObject):
    pass


class SQLMatchParticipantTimeline(SQLBaseObject):
    _dto_type = MatchParticipantTimelineDto
    _table = Table("match_participant_timeline", metadata,
                   Column("match_platformId", String(5), primary_key=True),
                   Column("match_gameId", BigInteger, primary_key=True),
                   Column("match_participant_participantId", Integer, primary_key=True),
                   Column("lane", String(12)),
                   Column("role", String(12)),
                   ForeignKeyConstraint(
                       ["match_platformId", "match_gameId", "match_participant_participantId"],
                       ["match_participant.match_platformId", "match_participant.match_gameId",
                        "match_participant.participantId"]))
    _relationships = {"deltas": (SQLMatchParticipantTimelineDeltas, {})}

    def __init__(self, **kwargs):
        kwargs["deltas"] = [{"type": key, **value}
                            for key, value in kwargs.items()
                            if key.endswith("Deltas")]
        super().__init__(**kwargs)

    def to_dto(self):
        dto = super().to_dto()
        deltas = dto.pop("deltas")
        for delta in deltas:
            dto[delta["type"]] = {key: value for key, value in delta.items() if key != "type"}
        return dto


map_object(SQLMatchParticipantTimeline)


class MatchParticipantStatsDto(DtoObject):
    pass


class SQLMatchParticipantStats(SQLBaseObject):
    _table = Table("match_participant_stats", metadata,
                   Column("match_platformId", String(5), primary_key=True),
                   Column("match_gameId", BigInteger, primary_key=True),
                   Column("match_participant_participantId", Integer, primary_key=True),
                   Column("win", Boolean),
                   Column("kills", Integer),
                   Column("deaths", Integer),
                   Column("assists", Integer),
                   Column("champLevel", Integer),
                   Column("item0", Integer),
                   Column("item1", Integer),
                   Column("item2", Integer),
                   Column("item3", Integer),
                   Column("item4", Integer),
                   Column("item5", Integer),
                   Column("item6", Integer),
                   Column("visionScore", Integer),
                   Column("wardsPlaced", Integer),
                   Column("wardsKilled", Integer),
                   Column("visionWardsBoughtInGame", Integer),
                   Column("sightWardsBoughtInGame", Integer),
                   Column("goldEarned", Integer),
                   Column("goldSpent", Integer),
                   Column("totalDamageDealt", Integer),
                   Column("physicalDamageDealt", Integer),
                   Column("magicDamageDealt", Integer),
                   Column("trueDamageDealt", Integer),
                   Column("totalDamageDealtToChampions", Integer),
                   Column("physicalDamageDealtToChampions", Integer),
                   Column("magicDamageDealtToChampions", Integer),
                   Column("trueDamageDealtToChampions", Integer),
                   Column("damageDealtToObjectives", Integer),
                   Column("damageDealtToTurrets", Integer),
                   Column("totalDamageTaken", Integer),
                   Column("physicalDamageTaken", Integer),
                   Column("magicalDamageTaken", Integer),
                   Column("trueDamageTaken", Integer),
                   Column("damageSelfMitigated", Integer),
                   Column("totalHeal", Integer),
                   Column("totalUnitsHealed", Integer),
                   Column("totalMinionsKilled", Integer),
                   Column("neutralMinionsKilled", Integer),
                   Column("neutralMinionsKilledTeamJungle", Integer),
                   Column("neutralMinionsKilledEnemyJungle", Integer),
                   Column("turretKills", Integer),
                   Column("inhibitorKills", Integer),
                   Column("killingSprees", Integer),
                   Column("largestKillingSpree", Integer),
                   Column("largestMultKill", Integer),
                   Column("doubleKills", Integer),
                   Column("tripleKills", Integer),
                   Column("quadraKills", Integer),
                   Column("pentaKills", Integer),
                   Column("unrealKills", Integer),
                   Column("firstBloodKill", Boolean),
                   Column("firstBloodAssist", Boolean),
                   Column("firstTowerKill", Boolean),
                   Column("firstTowerAssist", Boolean),
                   Column("firstInhibitorKill", Boolean),
                   Column("firstInhibitorAssist", Boolean),
                   Column("largestCriticalStrike", Integer),
                   Column("longestTimeSpentLiving", Integer),
                   Column("totalTimeCrowdControlDealt", Integer),
                   Column("timeCCingOthers", Integer),
                   # TODO Seems like the next 3 stats are always 0, maybe we shouldn't stock them?
                   Column("totalPlayerScore", Integer),
                   Column("combatPlayerScore", Integer),
                   Column("totalScoreRank", Integer),
                   # TODO See the objective stats come from. Haven't seen the variable in soloQ games, mb Nexus Blitz?
                   Column("teamObjective", Integer),
                   Column("objectivePlayerScore", Integer),
                   Column("altarsCaptured", Integer),
                   Column("altarsNeutralized", Integer),
                   Column("nodeCapture", Integer),
                   Column("nodeCaptureAssist", Integer),
                   Column("nodeNeutralize", Integer),
                   Column("nodeNeutralizeAssist", Integer),
                   Column("statPerk0", Integer),
                   Column("statPerk1", Integer),
                   Column("statPerk2", Integer),
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
                       ["match_platformId", "match_gameId", "match_participant_participantId"],
                       ["match_participant.match_platformId", "match_participant.match_gameId",
                        "match_participant.participantId"]))
    _dto_type = MatchParticipantStatsDto


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
                       ["match_platformId", "match_gameId"],
                       ["match.platformId", "match.gameId"]))
    _relationships = {
        "stats": (SQLMatchParticipantStats, {"uselist": False}),
        "timeline": (SQLMatchParticipantTimeline, {"uselist": False})
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
                   Column("p_summonerName", String(30)),
                   Column("p_summonerId", String(63)),
                   Column("p_accountId", String(56)),
                   Column("p_currentAccountId", String(56)),
                   Column("p_profileIcon", Integer),
                   Column("p_matchHistoryUri", String(80)),
                   ForeignKeyConstraint(
                       ["p_currentPlatformId", "match_gameId"],
                       ["match.platformId", "match.gameId"]))

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
                newKey = key[2:]
                player[newKey] = dto.pop(key)
        dto["player"] = player
        # TODO Check behaviour, the Uri is supplied by Riot so why is it self-made here? Mb should cut the field
        # Create match history Uri
        #dto["player"]["matchHistoryUri"] = "/v1/stats/player_history/" + player["platformId"] + "/" + str(
        #    player["accountId"])
        return dto


map_object(SQLMatchParticipantsIdentities)


class MatchBanDto(DtoObject):
    pass


class SQLMatchBan(SQLBaseObject):
    _dto_type = MatchBanDto
    _table = Table("match_ban", metadata,
                   Column("match_platformId", String(5), primary_key=True),
                   Column("match_gameId", BigInteger, primary_key=True),
                   Column("pickTurn", Integer, primary_key=True),
                   Column("championId", Integer),
                   Column("teamId", Integer),
                   ForeignKeyConstraint(
                       ["match_platformId", "match_gameId", "teamId"],
                       ["match_team.match_platformId", "match_team.match_gameId", "match_team.teamId"]))


map_object(SQLMatchBan)


class MachTeamDto(DtoObject):
    pass


class SQLMatchTeam(SQLBaseObject):
    _dto_type = MachTeamDto
    _table = Table("match_team", metadata,
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
                       ["match_platformId", "match_gameId"],
                       ["match.platformId", "match.gameId"]))
    _relationships = {"bans": (SQLMatchBan, {})}

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
    _table = Table("match", metadata,
                   Column("platformId", String(5), primary_key=True),
                   Column("gameId", BigInteger, primary_key=True),
                   Column("seasonId", Integer),
                   Column("queueId", Integer),
                   Column("gameVersion", String(23)),
                   Column("mapId", Integer),
                   Column("gameDuration", Integer),
                   Column("gameCreation", BigInteger),
                   Column("lastUpdate", BigInteger))
    _relationships = {
        "teams": (SQLMatchTeam, {}),
        "participants": (SQLMatchParticipant, {"lazy": "selectin"}),
        "participantIdentities": (SQLMatchParticipantsIdentities, {})
    }
    _constants = ["gameType", "gameMode"]


map_object(SQLMatch)
