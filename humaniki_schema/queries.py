import sqlalchemy

from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.orm import aliased

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
        return get_aggregations_obj_normal(bias_value, dimension_values, session, as_subquery,
                                           create_if_no_exist)


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

    # add the dimensional constraints
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


def get_aggregations_obj_normal(bias_value, dimension_values, session, as_subquery, create_if_no_exist):
    """
    """
    #  please check that at least one of bias_value or dimension_values is nonempty.
    assert not ((bias_value is None) and (dimension_values is None))
    if bias_value is not None:
        # there may or may not be a limitation on the bias value
        if isinstance(bias_value, dict):
            bias_prop, bias_qid = list(bias_value.items())[0][0][1]
        else:
            # guessing we are taking a shortcut and just have the qid, and we're still in the gender realm
            # TODO never come here
            bias_prop, bias_qid = 21, bias_value
    else:
        bias_prop, bias_value = None, None

    dimension_val_list = [(bias_prop, bias_value)]

    # add the dimensional constraints
    # recall that in python 3.7+ dictionaries keep insertion order
    if isinstance(dimension_values, dict):
        dimension_val_list += list(dimension_values.items())
    else:
        raise ReferenceError("I don't know what you're attempting to query on")

    # build the query with an accumulator pattern
    aggregations_id_q = session.query(metric_aggregations_n)
    for pos, (agg_prop, agg_val) in enumerate(dimension_val_list):
        # initialize an aliased table that we'll be joining on
        a_man = aliased(metric_aggregations_n)
        # these 'all' strings may come come
        if agg_val == 'all' or agg_val is None:
            continue  # hope there is no value called all
        # in cases where we are searching a range we may get a function of the aliased table
        elif callable(agg_val):
            value_filter = agg_val(a_man.value)
        # otherwise likely we are doing a straight comparison
        else:
            agg_val = get_exact_project_id(session,
                                           agg_val) if agg_prop == hs_utils.Properties.PROJECT.value else agg_val
            value_filter = a_man.value == agg_val

        aggregations_id_q = aggregations_id_q.join(a_man, metric_aggregations_n.id == a_man.id) \
            .filter(a_man.property == agg_prop) \
            .filter(value_filter) \
            .filter(a_man.aggregation_order == pos)

    if as_subquery:
        return aggregations_id_q.subquery('aggs')

    # print(aggregations_id_q)
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


def get_exact_project_id(session, exact_proj_code):
    q = session.query(project.id).filter(project.code == exact_proj_code)
    proj_id = q.scalar()
    return proj_id


class AggregationIdGetter():
    """
    Useful to get many aggregation ids via cacheing, like for inserting during generation
    """

    def __init__(self, bias, props, session=None):
        self.bias = bias
        self.props = hs_utils.order_props(props)
        self.all_props = [bias] + props
        self.props_values = [p.value for p in self.all_props]
        # not sure if this should be in the init fn
        self.session = session if session else session_factory()

    def get_all_known_aggregations_of_props(self):
        # need as n+1 many aliased versions of m_a_n as there are properties including gender
        # the +1 comes from the fact that we need to verify that this is unique combination of ids
        # build the query with an accumulator pattern
        # select
        #       *
        #       from metric_aggregations_n a1
        #       join metric_aggregations_n a2
        #         on a1.id = a2.id
        #       join metric_aggregations_n a3
        #         on a2.id=a3.id
        #       left outer join metric_aggregations_n a4
        #         on a3.id=a4.id and a4.property not in (0,21,27)
        #       where a1.property = 21
        #         and a1.aggregation_order = 0
        #         and a2.property = 0
        #         and a2.aggregation_order = 1
        #         and a3.property = 27
        #         and a3.aggregation_order = 2
        #         and a4.id is null

        num_mans = len(self.all_props) + 1
        # a_mans = [aliased(metric_aggregations_n, alias=f'a{i}') for i in range(num_mans)]
        a_mans = [aliased(metric_aggregations_n) for i in range(num_mans)]
        agg_q = self.session.query(*a_mans)
        for i, a_man in enumerate(a_mans):
            is_first_table = i == 0
            is_last_table = i == num_mans-1

            join_type = 'join' if not is_last_table else 'outerjoin'
            # ie. a2.id = a1.id
            default_join = a_mans[i-1].id == a_man.id
            last_join = and_(default_join, ~a_man.property.in_(self.props_values))
            join_statement = default_join if not is_last_table else last_join
            where_statement = a_man.property == self.props_values[i] if not is_last_table else a_man.id == None
            # add the join statement if it's not the first table
            if not is_first_table:
                agg_q = getattr(agg_q, join_type)(a_man, join_statement)
            # always add the where statement
            agg_q = agg_q.filter(where_statement)

        known_aggregations = agg_q.all()
        print(f'found {len(known_aggregations)} known aggregations')
        self.known_aggregations = known_aggregations
        return known_aggregations

    def lookup(self, bias_value, dimension_values):
        return
