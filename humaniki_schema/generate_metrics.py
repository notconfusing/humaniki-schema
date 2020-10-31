import sqlalchemy
import sys
import time
from sqlalchemy import create_engine, func, and_, or_
import datetime
import json
import os

from humaniki_schema.generate_insert import insert_data
from humaniki_schema.queries import get_properties_obj, get_aggregations_obj, get_latest_fill_id
from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
    metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    project
from humaniki_schema.db import session_factory
from humaniki_schema.db import engine as db_engine
import humaniki_schema.utils as hs_utils
import pandas as pd
import numpy as np


class MetricFactory():
    """
    This class is responsible for
    # load config file
    # generate permutations
    # creating metricCreator objects
      - executing them
    - creating the aggregation_id and property_id tables first if necessary, or cacheing them in memory for fast access
    """

    def __init__(self, config):
        self.fill_id = None


class MetricCreator():
    """
    Create a single "metric" (expands to multiple 'metric' rows) based on a population_defintion, bias property, dimension, property, and thresholds
    """

    def __init__(self, population_definition, bias_property, dimension_properties, thresholds, fill_id):
        self.population_definition = population_definition
        self.coverage_q = None
        self.bias_property = bias_property
        self.dimension_properties = dimension_properties
        self.join_dicts = {}
        self.aggregation_ids = None
        self.properties_ids = None
        self.thresholds = thresholds
        self.fill_id = fill_id

    def compile(self):
        pass

    def execute(self):
        pass

    def persist(self):
        pass

    def run(self):
        self.compile()
        self.execute()
        self.persist()


if __name__ == '__main__':
    mf = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'])
