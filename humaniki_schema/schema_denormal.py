# example taken from http://pythoncentral.io/introductory-tutorial-python-sqlalchemy/
import os
import sys
from sqlalchemy import Column, Integer, SMALLINT, String, Text, DateTime, Date, Enum, Boolean, BigInteger, Index, Float, ForeignKey, JSON
from sqlalchemy.dialects.mysql import MEDIUMTEXT, LONGTEXT, TINYTEXT, TEXT, VARCHAR, TINYINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
import sqlalchemy
import datetime

Base = declarative_base()

class fill(Base):
    __tablename__ = 'fill'
    id                  = Column(Integer, primary_key=True)
    date                = Column(Date, index=True)
    type                = Column(TINYINT) #an enum
    detail              = Column(JSON) # include whether this is a front or back fill and provenance

class metric(Base):
    __tablename__ = 'metric'
    # TODO: get partitioning to work
    # __table_args__ = {'mysql_partition_by': "KEY(id, fill_id)",
    #                   'mysql_engine': 'InnoDB',
    #                  'mysql_charset':'utf8mb4'}
    # TODO create a compound id from fill_id, facet, population, properties_id, agg_vals_id
    # __table_args__ = (Index('my_index', "a", "b"), )

    id                  = Column(Integer, primary_key=True)
    fill_id             = Column(Integer, ForeignKey("fill.id"), index=True)
    facet               = Column(TINYINT, index=True)
    population          = Column(TINYINT, index=True)
    properties_id       = Column(Integer, ForeignKey('metric_properties.id'), index=True)
    aggregation_vals    = Column(Integer, ForeignKey('metric_aggregations.id'))
    bias_value          = Column(Integer)
    total               = Column(Integer)
    # TODO compound index

class metric_properties(Base):
    __tablename__ = 'metric_properties'
    id                  = Column(Integer, primary_key=True)
    properties          = Column(JSON) # list of p-values of the properties in ascending order # might want SET

class metric_aggregations(Base):
    __tablename__ = 'metric_aggregations'
    id                  = Column(Integer, primary_key=True)
    aggregations        = Column(JSON) # ordered dict {property:val}, ordered by keys

class metric_coverage(Base):
    __tablename__ = 'metric_coverage'
    id                  = Column(Integer, primary_key=True)
    total_with_properties = Column(Integer)
    total_sitelinks_with_properties = Column(Integer)

class human(Base):
    __tablename__ = 'human'
    id                   = Column(Integer, primary_key=True)
    fill_id              = Column(Integer, ForeignKey("fill.id"), index=True)
    qid                  = Column(Integer, index=True)
    year_of_birth        = Column(SMALLINT)
    year_of_death        = Column(SMALLINT)
    gender               = Column(Integer)
    sitelink_count       = Column(SMALLINT)

class human_country(Base):
    __tablename__ = 'human_country'
    id                   = Column(Integer, primary_key=True)
    fill_id       = Column(Integer, ForeignKey('fill.id'), index=True)
    human_id           = Column(Integer, ForeignKey('human.qid'))
    country       = Column(Integer)

class human_occupation(Base):
    __tablename__ = 'human_occupation'
    id                   = Column(Integer, primary_key=True)
    fill_id            = Column(Integer, ForeignKey('fill.id'), index=True)
    human_id           = Column(Integer, ForeignKey('human.qid'))
    occupation         = Column(Integer)

class human_property(Base):
    __tablename__ = 'human_property'
    id                   = Column(Integer, primary_key=True)
    fill_id            = Column(Integer, ForeignKey('fill.id'), index=True)
    human_id           = Column(Integer, ForeignKey('human.qid'))
    property_p         = Column(Integer)

class human_sitelink(Base):
    __tablename__ = 'human_sitelink'
    id                   = Column(Integer, primary_key=True)
    fill_id            = Column(Integer, ForeignKey('fill.id'), index=True)
    human_id           = Column(Integer, ForeignKey('human.qid'))
    sitelink           = Column(TINYTEXT)

class occupation_parent(Base):
    __tablename__ = 'occupation_parent'
    id                   = Column(Integer, primary_key=True)
    fill_id            = Column(Integer, ForeignKey('fill.id'), index=True)
    occupation         = Column(Integer)
    parent             = Column(Integer)


class label(Base):
    __tablename__ = 'label'
    id                   = Column(Integer, primary_key=True)
    fill_id            = Column(Integer, ForeignKey('fill.id'), index=True)
    qid                = Column(Integer, index=True)
    lang               = Column(TINYTEXT)
    label              = Column(MEDIUMTEXT) # OR TINYTEXT
