import datetime, threading, time

from abc import abstractmethod
from typing import Mapping
from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import mapper, relationship, reconstructor
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
                setattr(self, key + "Id", Constant.create(value).id)
            else:
                setattr(self, key, value)
    @reconstructor
    def init_on_load(self):       
        if hasattr(self, "_constants"):
            for constant in self._constants:
                setattr(self, constant, Constant.create(None, getattr(self, constant + "Id")).value)       
 
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
                    map[constant] = value
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
                return now > (self.lastUpdate if self.lastUpdate else 0) + expire_seconds
        return False

    def updated(self):
        if hasattr(self, "lastUpdate"):
            self.lastUpdate = datetime.datetime.now().timestamp()

    @classmethod
    def _create_properties(cls):
        prop = {}
        if hasattr(cls, '_relationships'):
            for key, value in cls._relationships.items():
                if not "lazy" in value[1]:
                    value[1]["lazy"] = "joined"
                prop[key] = relationship(value[0], cascade="all, delete-orphan",**value[1])
        if hasattr(cls, '_constants'):
            for key in cls._constants:
                column_name = key + "Id"
                if not column_name in cls._table.c:
                    cls._table.append_column(Column(column_name, Integer))                             
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

class Constant():
    _session = None
    _lock = threading.Lock()
    _cache_by_value = {}
    _cache_by_id = {}
    @classmethod
    def create(cls,value=None,id=None):
        if value == "" and not id:
            raise ValueError("Either value or id must be provided")
        elif value and id:
            return cls(value, id)
        elif value:
            if value in cls._cache_by_value:
                return cls(value, cls._cache_by_value[value])
            else:
                session = cls._session()
                const = session.query(SQLConstant).filter_by(value=value).first()
                if not const:
                    const = SQLConstant(value)
                    session.add(const)
                    session.commit()
                cls._cache_by_value[value] = const.id
                cls._cache_by_id[const.id] = value
                return cls(const.value, const.id)
        elif id:
            if id in cls._cache_by_id:
                return cls(cls._cache_by_id[id], id)
            else:
                session = cls._session()
                const = session.query(SQLConstant).filter_by(id=id).first()
                cls._cache_by_value[const.value] = const.id
                cls._cache_by_id[const.id] = const.value
                return cls(const.value, const.id)
        else:
            # The constant is None return it with id -1
            return cls(value, -1)
    def __init__(self, value, id):
        self.value = value
        self.id = id

    def to_dto(self):
        return self.value

class SQLConstant(SQLBaseObject):
    _dto_type = ConstantDto
    _table = Table("constant", metadata,
                    Column("id", Integer, primary_key=True, autoincrement=True),
                    Column("value", String(30), unique=True))

    def __init__(self, constant, id=None):
        setattr(self, "value", constant)
        setattr(self, "id", id)

    def to_dto(self):
       return getattr(self,"value")

map_object(SQLConstant)
