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
import humaniki_schema.utils as hs_utils

Base = declarative_base()

class fill(Base):
    __tablename__ = 'fill'
    id                  = Column(Integer, primary_key=True)
    date                = Column(Date, index=True)
    type                = Column(TINYINT) #an enum
    detail              = Column(JSON) # include whether this is a front or back fill and provenance


class metric(Base):
    __tablename__ = 'metric'
    __table_args__ = {'mysql_partition_by': "KEY(fill_id)"}
    # __table_args__ = {'mysql_partition_by': "KEY(fill_id, population, properties_id, aggregation_vals)"}
    # NOTE, will I regret leaving out a facet column, or is everything unique identified by
    fill_id             = Column(Integer, primary_key=True)
    population_id       = Column(TINYINT, primary_key=True)
    properties_id       = Column(Integer, primary_key=True)
    aggregations_id     = Column(Integer, primary_key=True)
    bias_value          = Column(Integer)
    total               = Column(Integer)

    def to_dict(self):
        return {"fill_id": self.fill_id,
                "population_id": hs_utils.PopulationDefinition(self.population_id).name,
                "properties_id": self.properties_id,
                "aggregations_id": self.aggregations_id,
                "bias_value": self.bias_value,
                "total": self.total,
                }

class metric_properties_j(Base):
    __tablename__ = 'metric_properties_j'
    id                  = Column(Integer, primary_key=True)
    bias_property       = Column(Integer) # the bias property, eg gender
    properties          = Column(JSON) # list of p-values of the properties in ascending order
    properties_len      = Column(TINYINT)


class metric_aggregations_j(Base):
    __tablename__ = 'metric_aggregations_j'
    id                  = Column(Integer, primary_key=True)
    bias_value          = Column(Integer) # is the value of the bias property
    aggregations        = Column(JSON) # ordered list to zip with properties to get {property:val}
    aggregations_len    = Column(TINYINT)


class metric_properties_n(Base):
    __tablename__ = 'metric_properties_n'
    id                  = Column(Integer, primary_key=True)
    property            = Column(Integer, primary_key=True)
    property_order      = Column(TINYINT, primary_key=True)


class metric_aggregations_n(Base):
    __tablename__ = 'metric_aggregations_n'
    id                  = Column(Integer, primary_key=True)
    property            = Column(Integer, primary_key=True)
    value               = Column(Integer, primary_key=True)
    aggregation_order   = Column(Integer, primary_key=True)


class metric_coverage(Base):
    __tablename__ = 'metric_coverage'
    fill_id                         = Column(Integer, ForeignKey('fill.id'), primary_key=True)
    properties_id                   = Column(Integer, primary_key=True)
    total_with_properties           = Column(Integer)
    total_sitelinks_with_properties = Column(Integer)


class human(Base):
    __tablename__ = 'human'
    fill_id              = Column(Integer, ForeignKey("fill.id"), primary_key=True)
    qid                  = Column(Integer, primary_key=True)
    year_of_birth        = Column(SMALLINT)
    year_of_death        = Column(SMALLINT)
    gender               = Column(Integer)
    sitelink_count       = Column(SMALLINT)

class human_country(Base):
    __tablename__ = 'human_country'
    fill_id            = Column(Integer, primary_key=True)
    human_id           = Column(Integer, primary_key=True)
    country            = Column(Integer, primary_key=True)

class human_occupation(Base):
    __tablename__ = 'human_occupation'
    fill_id            = Column(Integer, ForeignKey('fill.id'), primary_key=True)
    human_id           = Column(Integer, primary_key=True)
    occupation         = Column(Integer, primary_key=True)

class human_property(Base):
    __tablename__ = 'human_property'
    fill_id            = Column(Integer, ForeignKey('fill.id'), primary_key=True)
    human_id           = Column(Integer, primary_key=True)
    property_p         = Column(Integer, primary_key=True)


class human_sitelink(Base):
    __tablename__ = 'human_sitelink'
    fill_id            = Column(Integer, ForeignKey('fill.id'), primary_key=True)
    human_id           = Column(Integer, primary_key=True)
    sitelink           = Column(VARCHAR(32), primary_key=True)


class occupation_parent(Base):
    __tablename__ = 'occupation_parent'
    fill_id            = Column(Integer, ForeignKey('fill.id'), primary_key=True)
    occupation         = Column(Integer, primary_key=True)
    parent             = Column(Integer, primary_key=True)


class label(Base):
    __tablename__ = 'label'
    id                 = Column(Integer, primary_key=True)
    fill_id            = Column(Integer, ForeignKey('fill.id'))
    qid                = Column(Integer)
    lang               = Column(VARCHAR(32))
    label              = Column(VARCHAR(512)) # OR TINYTEXT
    property           = Column(Integer, index=True)


class project(Base):
    __tablename__ = 'project'
    id                 = Column(Integer, primary_key=True)
    code               = Column(VARCHAR(32), unique=True, index=True)
    type               = Column(TINYTEXT)
    label              = Column(TINYTEXT)
    url                = Column(TINYTEXT)

