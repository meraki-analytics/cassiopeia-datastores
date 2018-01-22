import datetime, threading

from abc import abstractmethod
from typing import Mapping
from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relationship
from cassiopeia.dto.common import DtoObject
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
            elif hasattr(self, "_constants") and key in self._constants:
                # Create constant object for sqlalchemy
                setattr(self, key, SQLConstant.create(value))
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
        if hasattr(self, "_constants"):
            for constant in self._constants:
                value = getattr(self, constant)
                if value:
                    map[constant] = value.to_dto()
                    del map[constant + "Id"]
                else:
                    map[constant] = None
                    del map[constant + "Id"]
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
                prop[key] = relationship(value[0], lazy="joined", cascade="all, delete-orphan",**value[1])
        if hasattr(cls, '_constants'):
            for key in cls._constants:
                column_name = key + "Id"
                if not column_name in cls._table.c:
                    cls._table.append_column(Column(column_name, Integer, ForeignKey("constant.id")))
                prop[key] = relationship(SQLConstant, lazy="joined", cascade="all", foreign_keys=[cls._table.c[column_name]])                                
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

class ConstantDto(DtoObject):
    pass

class SQLConstant(SQLBaseObject):
    # This session is used to make get_or_create() possible
    # It will be set by the SQLStore on creation
    _session = None
    _lock = threading.Lock()
    _cache = {}
    _dto_type = ConstantDto
    _table = Table("constant", metadata,
                    Column("id", Integer, primary_key=True, autoincrement=True),
                    Column("value", String(30), unique=True))

    @classmethod
    def create(cls, value):
        with cls._lock:
            if value in cls._cache:
                return cls(value, cls._cache[value])
            session = cls._session()
            try:
                instance = session.query(cls).filter_by(value=value).first()
                if instance:
                    cls._cache[value] = instance
                    print("Found constant",instance.to_dto())
                    return instance
                else:
                    instance = cls(value)
                    session.add(instance)
                    session.commit()
                    cls._cache[value] = instance.id
                    print("Created constant", instance.to_dto())
                    return instance
            except Exception as e:
                print(e)
                session.rollback()
                return None            
            finally:
                print("Session closed")
                session.close()

    def __init__(self, constant, id=None):
        setattr(self, "value", constant)
        setattr(self, "id", id)

    def to_dto(self):
       return getattr(self,"value")

map_object(SQLConstant)
