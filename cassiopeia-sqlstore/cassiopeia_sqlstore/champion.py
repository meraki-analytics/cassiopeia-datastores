from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, ForeignKeyConstraint

from cassiopeia.dto.champion import ChampionRotationDto
from cassiopeia.dto.common import DtoObject

from .common import metadata, SQLBaseObject, map_object

class ChampionRotationChampionDto(DtoObject):
    pass

class SQlChampionRotationChampion(SQLBaseObject):
    _dto_type = ChampionRotationChampionDto
    _table = Table("championrotation_champion", metadata,
                Column("id", Integer, primary_key=True),
                Column("platform", String(5), primary_key=True),
                Column("rotationKeyId", Integer, primary_key=True),
                ForeignKeyConstraint(
                    ["platform"],
                    ["championrotation.platform"]
                ))
    _constants = ["rotationKey"]

map_object(SQlChampionRotationChampion)

class SQlChampionRotation(SQLBaseObject):
    _dto_type = ChampionRotationDto
    _table = Table("championrotation", metadata,
                Column("platform", String(5), primary_key=True),
                Column("maxNewPlayerLevel", Integer),
                Column("lastUpdate", BigInteger))
    _relationships = {"champions": (SQlChampionRotationChampion, {})}

    def __init__(self, **kwargs):
        kwargs["champions"] = [{'rotationKey':key, 'id': champ, 'platform':kwargs['platform']}
                        for key, value in kwargs.items() if not key in ["maxNewPlayerLevel", "platform", "region"]
                        for champ in value]
        super().__init__(**kwargs)

    def to_dto(self):
        dto = super().to_dto()
        for champ in dto.pop("champions"):
            if champ["rotationKey"] not in dto:
                dto[champ["rotationKey"]] = list()
            dto[champ["rotationKey"]].append(champ['id'])
        return dto

map_object(SQlChampionRotation)