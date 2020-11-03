import os
import sqlalchemy

from sqlalchemy import func, and_

from humaniki_schema.queries import get_latest_fill_id
from humaniki_schema.db import session_factory
from humaniki_schema.schema import human, human_sitelink, human_country, human_occupation
from humaniki_schema.utils import Properties, PopulationDefinition


class MetricFactory():
    """
    This class is responsible for
    # load config file
    # generate permutations
    # creating metricCreator objects
      - executing them
    - creating the aggregation_id and property_id tables first if necessary, or cacheing them in memory for fast access
    """

    def __init__(self, config, db_session=None):
        self.config = config
        self.db_session = db_session if db_session else session_factory()
        curr_fill, curr_fill_date = get_latest_fill_id(self.db_session)
        self.curr_fill = curr_fill
        self.curr_fill_date = curr_fill_date


class MetricCreator():
    """
    Create a single "metric" (expands to multiple 'metric' rows) based on a population_defintion, bias property, dimension, property, and thresholds
    """
    def __init__(self, population_definition, bias_property, dimension_properties, thresholds, fill_id, db_session):
        self.population_definition = population_definition
        self.coverage_q = None
        self.bias_property = bias_property
        self.dimension_properties = dimension_properties
        self.aggregation_ids = None
        self.properties_ids = None
        self.thresholds = thresholds
        self.fill_id = fill_id
        self.db_session = db_session
        self.metric_q = None
        self.metric_res = None

    def _get_dim_cols_from_dim_props(self):
        """
        note this is for a list of props
        """
        col_map = {Properties.PROJECT: human_sitelink.sitelink,
                   Properties.CITIZENSHIP: human_country.country,
                   Properties.DATE_OF_BIRTH: human.year_of_birth,
                   Properties.DATE_OF_DEATH: human.year_of_death,
                   Properties.OCCUPATION: human_occupation.occupation}
        return [col_map[p] for p in self.dimension_properties]

    def _get_dim_join_from_dim_prop(self, prop):
        """
        note this is for a single prop, not a list of props
        """
        join_table_map = {Properties.PROJECT: human_sitelink,
                          Properties.CITIZENSHIP: human_country,
                          Properties.OCCUPATION: human_occupation,
                          Properties.DATE_OF_BIRTH: None,
                          Properties.DATE_OF_DEATH: None, }
        join_table = join_table_map[prop]
        if join_table is None:  # not a join
            return None, None
        else:
            return join_table, and_(human.qid == join_table.human_id,
                                    human.fill_id == join_table.fill_id)

    def _get_dim_filter_from_dim_prop(self, dim_prop):
        filter_map = {Properties.DATE_OF_BIRTH:}

    def compile(self):
        # make something approximating
        # metric_q = db_session.query(human.gender, human_sitelink.sitelink, human_country.country,
        #                             func.count(human.gender)) \
        #     .join(human_country, and_(human.qid == human_country.human_id, human.fill_id == human_country.fill_id)) \
        #     .join(human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)) \
        #     .group_by(human_country.country, human_sitelink.sitelink, human.gender)
        bias_col = human.gender  # maybe getattr(human, bias_property.name.lower()
        count_col = func.count(bias_col)
        dim_cols = self._get_dim_cols_from_dim_props()
        metric_cols = [bias_col, *dim_cols, count_col]
        group_bys = [bias_col] + dim_cols

        metric_q = self.db_session.query(*metric_cols)
        for dim_prop in self.dimension_properties:
            ## apply joins
            join_table, join_on = self._get_dim_join_from_dim_prop(dim_prop)
            if join_table is not None:
                metric_q = metric_q.join(join_table, join_on)
            ## apply filters
            filter = self._get_dim_filter_from_dim_prop(dim_prop)
            if filter is not None:
                metric_q = metric_q.filter(filter)

        metric_q = metric_q.filter(human.fill_id == self.fill_id)
        metric_q = metric_q.group_by(group_bys)
        self.metric_q = metric_q

    def execute(self):
        try:
            self.metric_res = self.metric_q.all()
        except sqlalchemy.exc.error:
            raise

    def persist(self):
        pass

    def run(self):
        self.compile()
        self.execute()
        self.persist()


if __name__ == '__main__':
    mf = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'])
    # in the future metricfactor should fan this out to metriccreator via the config
    mc = MetricCreator(population_definition=PopulationDefinition.GTE_ONE_SITELINK,
                       bias_property=Properties.GENDER,
                       dimension_properties=[Properties.PROJECT, Properties.CITIZENSHIP],
                       fill_id=mf.curr_fill,
                       thresholds=None,
                       db_session=mf.db_session)
    mc.run()
