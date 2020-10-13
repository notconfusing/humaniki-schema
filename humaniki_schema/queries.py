import sqlalchemy

from sqlalchemy import create_engine, func, and_, or_
from humaniki_schema.generate_insert import insert_data
from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
    metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    project
import humaniki_schema.utils as hs_utils

from humaniki_schema.db import session_factory

db_session = session_factory()


def create_properties_obj_json(bias_property, dimension_properties, session):
    a_metric_properties = metric_properties_j(properties=dimension_properties,
                                              properties_len=len(dimension_properties),
                                              bias_property=bias_property)
    session.add(a_metric_properties)
    session.commit()
    return a_metric_properties

def get_properties_obj_json(bias_property, dimension_properties, session, as_subquery, create_if_no_exist):
    properties_id_q = session.query(metric_properties_j) \
        .filter(metric_properties_j.bias_property == bias_property) \
        .filter(metric_properties_j.properties_len == len(dimension_properties))
    for pos, prop_num in enumerate(dimension_properties):
        properties_id_q = properties_id_q.filter(metric_properties_j.properties[pos] == prop_num)

    if as_subquery:
        return properties_id_q.subquery('props')

    try:
        properties_id_obj = properties_id_q.one()
        return properties_id_obj
    except sqlalchemy.orm.exc.NoResultFound as e:
        if create_if_no_exist:
            return create_properties_obj_json(bias_property, dimension_properties, session)
        else:
            raise


def get_properties_obj(bias_property, dimension_properties, session=None, table=metric_properties_j,
                       as_subquery=False, create_if_no_exist=False):
    if session is None:
        session = db_session
    if table == metric_properties_j:
        return get_properties_obj_json(bias_property, dimension_properties, session, as_subquery, create_if_no_exist)
    if table == metric_properties_n:
        raise NotImplementedError('Coming soon if needed')


def get_aggregations_ids(session, ordered_query_params):
    # aggregations_id is None indicates there's no constraint on the aggregation_id
    ordered_aggregations = ordered_query_params.values()
    if all([v == 'all' for v in ordered_aggregations]):
        return None
    else:
        aggregations_id_q = session.query(metric_aggregations_j.id)
        for pos, agg_val in enumerate(ordered_aggregations):
            if agg_val != 'all':  # hope there is no value called all
                aggregations_id_q = aggregations_id_q.filter(metric_aggregations_j.aggregations[pos] == agg_val)
        print(f"aggregations query {aggregations_id_q}")
        # TODO see if using subqueries is faster
        aggregations_id_subquery = aggregations_id_q.subquery()
        aggregations_id = aggregations_id_q.all()
        print(f"aggregations_id is: {aggregations_id}")
        return aggregations_id
