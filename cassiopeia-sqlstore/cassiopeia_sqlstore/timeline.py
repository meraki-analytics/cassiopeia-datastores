from sqlalchemy import Table, Column, Integer, String, BigInteger, ForeignKeyConstraint, PickleType

from cassiopeia.dto.common import DtoObject
from cassiopeia.dto.match import TimelineDto

from .common import metadata, SQLBaseObject, map_object

class TimelineFrameEventDto(DtoObject):
    pass

class SQLTimelineFrameEvent(SQLBaseObject):
    _dto_type = TimelineFrameEventDto
    _table = Table("match_timeline_frame_event", metadata,
                    Column("match_timeline_matchId", BigInteger, primary_key=True),
                    Column("match_timeline_platformId", String(5), primary_key=True),
                    Column("match_timeline_frame_id", Integer, primary_key=True),
                    Column("id", Integer, primary_key=True),
                    Column("timestamp", Integer),
                    Column("participantId", Integer),
                    Column("itemId", Integer),
                    Column("skillSlot", Integer),
                    Column("creatorId", Integer),
                    Column("teamId", Integer),
                    Column("killerId", Integer),
                    Column("pointCaptured", String(20)),
                    Column("victimId", Integer),
                    Column("afterId", Integer),
                    Column("beforeId", Integer),
                    Column("position_x", Integer),
                    Column("position_y", Integer),
                    Column("assistingParticipantIds", PickleType),
                    ForeignKeyConstraint(
                        ["match_timeline_matchId", "match_timeline_platformId", "match_timeline_frame_id"],
                        ["match_timeline_frame.match_timeline_matchId", "match_timeline_frame.match_timeline_platformId", "match_timeline_frame.id"]
                    ))
    _constants = ["type","wardType","levelUpType","eventType","towerType","monsterType","monsterSubType","buildingType"]
    def __init__(self, **kwargs):
        if "position" in kwargs:
            kwargs["position_x"] = kwargs["position"]["x"]
            kwargs["position_y"] = kwargs["position"]["y"]
        super().__init__(**kwargs)

    def to_dto(self):
        dto = super().to_dto()
        if "position_x" in dto and dto["position_x"] is not None:
            dto["position"] = {"x":dto["position_x"], "y":dto["position_y"]}
        dto = self._dto_type(**{key:value for key, value in dto.items() if value is not None})
        return dto

map_object(SQLTimelineFrameEvent)

class TimelineParticipantFrameDto(DtoObject):
    pass

class SQlTimelineParticipantFrame(SQLBaseObject):
    _dto_type = TimelineParticipantFrameDto
    _table = Table("match_timeline_frame_participant", metadata,
                    Column("match_timeline_matchId", BigInteger, primary_key=True),
                    Column("match_timeline_platformId", String(5), primary_key=True),
                    Column("match_timeline_frame_id", Integer, primary_key=True),
                    Column("participantId", Integer, primary_key=True),
                    Column("currentGold", Integer),
                    Column("totalGold", Integer),
                    Column("level", Integer),
                    Column("xp", Integer),
                    Column("minionsKilled", Integer),
                    Column("jungleMinionsKilled", Integer),
                    Column("dominionScore", Integer),
                    Column("teamScore", Integer),
                    Column("position_x", Integer),
                    Column("position_y", Integer),
                    ForeignKeyConstraint(
                        ["match_timeline_matchId", "match_timeline_platformId", "match_timeline_frame_id"],
                        ["match_timeline_frame.match_timeline_matchId", "match_timeline_frame.match_timeline_platformId", "match_timeline_frame.id"]
                    ))

    def __init__(self, **kwargs):
        if not "position" in kwargs:
            kwargs["position_x"] = -1
            kwargs["position_y"] = -1
        else:
            kwargs["position_x"] = kwargs["position"]["x"]
            kwargs["position_y"] = kwargs["position"]["y"]
        super().__init__(**kwargs)

    def to_dto(self):
        result = super().to_dto()
        result["position"] = {"x":result["position_x"],"y":result["position_y"]}
        return result

map_object(SQlTimelineParticipantFrame)

class TimelineFrameDto(DtoObject):
    pass

class SQLTimelineFrame(SQLBaseObject):
    _dto_type = TimelineFrameDto
    _table = Table("match_timeline_frame", metadata,
                    Column("match_timeline_matchId", BigInteger, primary_key=True),
                    Column("match_timeline_platformId", String(5), primary_key=True),
                    Column("id", Integer, primary_key=True),
                    Column("timestamp", Integer),
                    ForeignKeyConstraint(
                        ["match_timeline_matchId", "match_timeline_platformId"],
                        ["match_timeline.matchId", "match_timeline.platformId"]
                    ))
    _relationships = {
                        "participantFrames":(SQlTimelineParticipantFrame,{"lazy":"selectin"}),
                        "events": (SQLTimelineFrameEvent, {"lazy":"selectin"})
                    }

    def __init__(self, **kwargs):
        kwargs["participantFrames"] = [value for key, value in kwargs["participantFrames"].items()]
        for i in range(0, len(kwargs["events"])):
            kwargs["events"][i]["id"] = i
        super().__init__(**kwargs)

    def to_dto(self):
        result = super().to_dto()
        result["participantFrames"] = {str(value["participantId"]):value for value in result["participantFrames"]}
        return result

map_object(SQLTimelineFrame)

class SQLTimeline(SQLBaseObject):
    _dto_type = TimelineDto
    _table = Table('match_timeline',metadata,
                    Column("matchId", BigInteger, primary_key=True),
                    Column("platformId", String(5), primary_key=True),
                    Column("frameInterval", Integer),
                    Column("lastUpdate", BigInteger))
    _relationships = {"frames":(SQLTimelineFrame,{"lazy":"selectin"})}
    def __init__(self, **kwargs):
        for i in range(0, len(kwargs["frames"])):
            kwargs["frames"][i]["id"] = i
        super().__init__(**kwargs)

map_object(SQLTimeline)
