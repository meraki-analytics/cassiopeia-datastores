import datetime

from abc import abstractmethod
from typing import Mapping
from sqlalchemy import MetaData
from sqlalchemy.orm import mapper, relationship

metadata = MetaData()

class SQLBaseObject(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, "_relationships") and key in self._relationships:
                #Create a new Object for that relation, so sqlalchemy knows how to handle it
                clazz = self._relationships[key][0]
                if type(value) is list:
                    setattr(self, key, [clazz(**v) for v in value])
                else:
                    setattr(self, key, clazz(**value))
            else:
                setattr(self, key, value)

    def to_dto(self):
        map = {}
        for column in self._table.columns:
            map[column.name] = getattr(self, column.name)
        # Go over relationships and convert them to a dto recursivly
        if hasattr(self, "_relationships"):
            for rel in self._relationships:
                value = getattr(self, rel)
                if isinstance(value, list):
                    map[rel] = [v.to_dto() for v in value]
                elif hasattr(value, "to_dto"):
                    map[rel] = value.to_dto()
                else:
                    map[rel] = value
        return self._dto_type(map)

    def has_expired(self, expirations: Mapping[type, float]) -> bool:
        if hasattr(self, "lastUpdate"):
            expire_seconds = expirations.get(self._dto_type,-1)
            if expire_seconds > 0:
                now = datetime.datetime.now().timestamp()
                return now > self.lastUpdate + expire_seconds
        return False

    def updated(self):
        if hasattr(self, "lastUpdate"):
            self.lastUpdate = datetime.datetime.now().timestamp()

    @classmethod
    def _create_properties(cls):
        prop = {}
        if hasattr(cls, '_relationships'):
            for key, value in cls._relationships.items():
                prop[key] = relationship(value[0],lazy="joined", cascade="all, delete-orphan",**value[1])
        return prop

    @classmethod
    def expire(cls, session, expirations: Mapping[type, float]):
        if "lastUpdate" in cls._table.columns:
            expire_seconds = expirations.get(cls._dto_type,-1)
            now = datetime.datetime.now().timestamp()
            session.query(cls).filter(cls.lastUpdate < now - expire_seconds).delete()
            session.commit()

    @abstractmethod
    def _table(self):
        pass

    @abstractmethod
    def _dto_type(self):
        pass

sql_classes = set()

def map_object(cls):
    # Add cls to set so they can be called to expire later on
    sql_classes.add(cls)
    properties = cls._create_properties()
    if not properties:
        mapper(cls,cls._table)
    else:
        mapper(cls,cls._table,properties=properties)
