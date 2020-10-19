import sqlalchemy

from sqlalchemy import create_engine, func, and_, or_
from humaniki_schema.generate_insert import insert_data
from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
    metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    project
import humaniki_schema.utils as hs_utils

from humaniki_schema.db import session_factory

db_session = session_factory()


def get_properties_obj(bias_property, dimension_properties, session=None, table=metric_properties_j,
                       as_subquery=False, create_if_no_exist=False):
    """
    the main entry point, get a property id, by the bias property (like gender) and the other dimensions
    the order we are storing the properties, either in json or normalized is
    0: bias_value, 1..n: properties sorted by their PID (and sitelink is faked as PID 0).

    overview
    get_property
        --> by json method, return or:
            --> if not exists create
                --> [subroutine A] create json with autoincrement to get an id, then
                    --> create normalized
        --> by normal method, return or:
            -->  if not exists create using [subroutine A]
    :param bias_property: the id of the main bias in question (eg. gender:21)
    :param dimension_properties: pre-sorted list of property ids
    :param session:
    :param table: either metric_properties_j or metric_properties_n
    :param as_subquery:
    :param create_if_no_exist:
    :return:
    """
    if session is None:
        session = db_session
    if table == metric_properties_j:
        return get_properties_obj_json(bias_property, dimension_properties, session, as_subquery, create_if_no_exist)
    elif table == metric_properties_n:
        raise NotImplementedError('Coming soon if needed')


def get_aggregations_obj(bias_value, dimension_values, session=None, table=metric_aggregations_j,
                         as_subquery=False, create_if_no_exist=False):
    """a very similar funciton to get_properties_obj. still not 100% if they should be abstracted. The key differences
    is that when the backend calls this function it may be querying for a return set of many aggregation_id. (E.g.
    all project='enwiki' which would return rows for ('male', 'enwiki') & ('female', 'enwiki'.)  ).
    On the other hand, metrics-generation may call this in order to figure out what the id is a specific aggregaitons combination.
    Therefore create-if-exists can only be done if all dimensional values are specified.
    :param bias_value: either a scalar the qid, eg. for male, or in the case of insertion a dict{bias_prop:bias_value}
    :param dimension_values: [val] list or {prop:val} dict. the sitelink, citizenship, year of birth, or other. dict is useful when creating.
    :param session:
    :param table: which table to query, json or normalized
    :param as_subquery:
    :param create_if_no_exist:
    :return:
    """
    if session is None:
        session = db_session
    if table == metric_aggregations_j:
        return get_aggregations_obj_json(bias_value, dimension_values, session, as_subquery,
                                         create_if_no_exist)
    elif table == metric_aggregations_n:
        raise NotImplementedError('Coming soon if needed')


def create_properties_obj(bias_property, dimension_properties, session):
    a_metric_properties_j = create_properties_obj_json(bias_property, dimension_properties, session)
    a_metric_properties_n = create_properties_obj_normal(prop_id=a_metric_properties_j.id,
                                                         bias_property=bias_property,
                                                         dimension_properties=dimension_properties,
                                                         session=session)
    return a_metric_properties_j


def create_properties_obj_json(bias_property, dimension_properties, session):
    a_metric_properties = metric_properties_j(properties=dimension_properties,
                                              properties_len=len(dimension_properties),
                                              bias_property=bias_property)
    session.add(a_metric_properties)
    session.commit()
    return a_metric_properties


def create_properties_obj_normal(prop_id, bias_property, dimension_properties, session):
    metric_properties_n_s = []
    for i, property in enumerate([bias_property, *dimension_properties]):
        a_metric_properties = metric_properties_n(id=prop_id,
                                                  property=property,
                                                  property_order=i)
        metric_properties_n_s.append(a_metric_properties)
    session.add_all(metric_properties_n_s)
    session.commit()
    return metric_properties_n_s


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
            return create_properties_obj(bias_property, dimension_properties, session)
        else:
            raise


def get_aggregations_obj_json(bias_value, dimension_values, session, as_subquery, create_if_no_exist):
    """
    Please check that at least one of bias_value or dimension_values is nonempty.
    """
    if bias_value is not None:
        # there may or may not be a limitation on the bias value
        bias_qid = list(bias_value.values())[0] if isinstance(bias_value, dict) else bias_value
        aggregations_id_q = session.query(metric_aggregations_j).filter(metric_aggregations_j.bias_value == bias_qid)
    else:
        aggregations_id_q = session.query(metric_aggregations_j)

    # add the dimensional values
    # recall that in python 3.7+ dictionaries keep insertion order
    dimension_val_list = dimension_values.values() if isinstance(dimension_values, dict) else dimension_values

    for pos, agg_val in enumerate(dimension_val_list):
        # these all strings come
        if agg_val == 'all':
            continue  # hope there is no value called all
        else:
            aggregations_id_q = aggregations_id_q.filter(metric_aggregations_j.aggregations[pos] == agg_val)

    if as_subquery:
        return aggregations_id_q.subquery('aggs')

    aggregations_id_objs = aggregations_id_q.all()
    if len(aggregations_id_objs) > 0:
        return aggregations_id_objs
    else:
        if create_if_no_exist:
            return create_aggregations_obj(bias_value, dimension_values, session)
        else:
            return aggregations_id_objs  # return a known empty list


def create_aggregations_obj(bias_value, dimension_aggregations, session):
    a_metric_aggregations_j = create_aggregations_obj_json(bias_value, dimension_aggregations, session)

    metric_aggregations_n = create_aggregations_obj_normal(agg_id=a_metric_aggregations_j.id,
                                                           bias_value=bias_value,
                                                           dimension_aggregations=dimension_aggregations,
                                                           session=session)
    return a_metric_aggregations_j


def create_aggregations_obj_json(bias_value, dimension_aggregations, session):
    bias_qid = list(bias_value.values())[0] if isinstance(bias_value, dict) else bias_value
    aggregations = list(dimension_aggregations.values()) if isinstance(dimension_aggregations,
                                                                       dict) else dimension_aggregations
    a_metric_aggregations = metric_aggregations_j(aggregations=aggregations,
                                                  aggregations_len=len(dimension_aggregations),
                                                  bias_value=bias_qid)
    session.add(a_metric_aggregations)
    session.commit()
    return a_metric_aggregations


def create_aggregations_obj_normal(agg_id, bias_value, dimension_aggregations, session):
    """
    we only create one aggregations-group (one agg_id) at a time.
    """
    # our order is that the first kv pair are the bias, and the the sorted dimensional aggregations
    aggregations_dict = {**bias_value, **dimension_aggregations}

    metric_aggregations_n_s = []
    for i, (prop_id, value) in enumerate(aggregations_dict.items()):
        value_code = value
        if prop_id == 0:
            # recall we fake sitelink as property id 0.
            value_code = get_project_internal_id_from_wikiencoding(value, session)
        a_metric_aggregations = metric_aggregations_n(id=agg_id,
                                                      property=prop_id,
                                                      value=value_code,
                                                      aggregation_order=i)
        metric_aggregations_n_s.append(a_metric_aggregations)
    session.add_all(metric_aggregations_n_s)
    session.commit()
    return metric_aggregations_n_s


def get_project_internal_id_from_wikiencoding(wikiencoding, session):
    # TODO maybe I should just set up a massive enum for this.
    project_q = session.query(project.id).filter_by(code=wikiencoding)
    return project_q.scalar()


def get_latest_fill_id(session):
    latest_q = session.query(func.max(fill.date)).subquery()
    q = session.query(fill.id, fill.date).filter(fill.date == latest_q)
    latest_fill_id, latest_fill_date = q.one()
    return latest_fill_id, latest_fill_date


def get_exact_fill_id(session, exact_fill_dt):
    q = session.query(fill.id, fill.date).filter(fill.date == exact_fill_dt)
    fill_id, fill_date = q.one()
    return fill_id, fill_date
