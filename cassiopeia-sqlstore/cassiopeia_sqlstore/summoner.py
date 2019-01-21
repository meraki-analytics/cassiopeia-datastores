from sqlalchemy import Table, Column, Integer, String, BigInteger

from cassiopeia.dto.summoner import SummonerDto

from .common import metadata, SQLBaseObject, map_object


class SQLSummoner(SQLBaseObject):
    _dto_type = SummonerDto
    _table = Table('summoner', metadata,
                   Column('platform', String(length=5), primary_key=True),
                   Column('id', String(63), primary_key=True),
                   Column('accountId', String(56)),
                   Column('puuid', String(78)),
                   Column('name', String(length=30)),
                   Column('summonerLevel', Integer),
                   Column('profileIconId', Integer),
                   Column('revisionDate', BigInteger)
                   # TODO Removed lastUpdate, need to make sure I'm not killing a feature
                   )


map_object(SQLSummoner)
