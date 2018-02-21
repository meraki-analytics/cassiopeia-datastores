from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, ForeignKeyConstraint, Text

from cassiopeia.dto.status import ShardStatusDto
from cassiopeia.dto.common import DtoObject

from cassiopeia.data import Region

from .common import metadata, SQLBaseObject, map_object

class ShardStatusIncidentUpdateTranslationDto(DtoObject):
    pass

class SQlShardStatusIncidentUpdateTranslation(SQLBaseObject):
    _dto_type = ShardStatusIncidentUpdateTranslationDto
    _table = Table("status_incident_update_translation", metadata,
                    Column("incident_id", String(24), primary_key=True),
                    Column("locale", String(8), primary_key=True),
                    Column("heading", Text),
                    Column("content", Text),
                    ForeignKeyConstraint(
                        ["incident_id"],
                        ["status_incident_update.id"]
                    ))

map_object(SQlShardStatusIncidentUpdateTranslation)

class ShardStatusIncidentUpdateDto(DtoObject):
    pass

class SQlShardStatusIncidentUpdate(SQLBaseObject):
    _dto_type = ShardStatusIncidentUpdateDto
    _table = Table("status_incident_update", metadata,
                    Column("id", String(24), primary_key=True),
                    Column("status_slug", String(5)),
                    Column("status_service_slug", String(10)),
                    Column("status_incident_id", Integer),
                    Column("author", String(20)),
                    Column("content", Text),
                    Column("severity", String(10)),
                    Column("created_at", String(80)),
                    Column("updated_at", String(80)),
                    ForeignKeyConstraint(
                        ["status_slug", "status_service_slug", "status_incident_id"],
                        ["status_incident.status_slug", "status_incident.status_service_slug", "status_incident.id"]
                    ))
    _relationships = {"translations":(SQlShardStatusIncidentUpdateTranslation, {})}

map_object(SQlShardStatusIncidentUpdate)

class ShardStatusIncidentDto(DtoObject):
    pass

class SQlShardStatusIncident(SQLBaseObject):
    _dto_type = ShardStatusIncidentDto
    _table = Table("status_incident", metadata,
                    Column("status_slug", String(5), primary_key=True),
                    Column("status_service_slug", String(10), primary_key=True),
                    Column("id", Integer, primary_key=True),
                    Column("active", Boolean),
                    Column("created_at", String(80)),
                    ForeignKeyConstraint(
                        ["status_slug", "status_service_slug"],
                        ["status_service.status_slug", "status_service.slug"]))
    _relationships = {"updates": (SQlShardStatusIncidentUpdate,{})}

map_object(SQlShardStatusIncident)

class ShardStatusServiceDto(DtoObject):
    pass

class SQLShardStatusService(SQLBaseObject):
    _dto_type = ShardStatusServiceDto
    _table = Table("status_service", metadata,
                    Column("status_slug", String(5), primary_key=True),
                    Column("slug", String(10), primary_key=True),
                    Column("name", String(20)),
                    ForeignKeyConstraint(
                        ["status_slug"],
                        ["status.slug"]))
    _relationships = {"incidents":(SQlShardStatusIncident,{})}

map_object(SQLShardStatusService)

class SQLShardStatus(SQLBaseObject):
    _dto_type = ShardStatusDto
    _table = Table("status", metadata,
                    Column("slug", String(5), primary_key=True),
                    Column("name", String(50)),
                    Column("region_tag", String(3)),
                    Column("hostname", String(50)),
                    Column("lastUpdate", BigInteger))
    _relationships = {"services":(SQLShardStatusService,{})}

    def to_dto(self):
        dto = super().to_dto()
        locales = set()
        locales.add(Region(dto["slug"].upper()).default_locale)
        for service in dto["services"]:
            for incident in service["incidents"]:
                for update in incident["updates"]:
                    for translation in update["translations"]:
                        locales.add(translation["locale"])
        dto["locales"] = list(locales)
        return dto

map_object(SQLShardStatus)
