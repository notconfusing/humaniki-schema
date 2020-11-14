import sqlalchemy

from sqlalchemy import func, and_
from sqlalchemy.orm import aliased

from humaniki_schema import utils as hs_utils
from humaniki_schema.schema import fill, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, \
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
    session.rollback()
    aggregations_id_objs = aggregations_id_q.all()
    session.commit()
    if len(aggregations_id_objs) > 0:
        return aggregations_id_objs
    else:
        if create_if_no_exist:
            return create_aggregations_obj(bias_value, dimension_values, session)
        else:
            return aggregations_id_objs  # return a known empty list


def create_aggregations_obj(bias_value, dimension_aggregations, session):
    a_metric_aggregations_j = create_aggregations_obj_json(bias_value, dimension_aggregations, session)

    try:
        metric_aggregations_n = create_aggregations_obj_normal(agg_id=a_metric_aggregations_j.id,
                                                           bias_value=bias_value,
                                                           dimension_aggregations=dimension_aggregations,
                                                           session=session)
    except NoSuchWikiError:
        raise

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
            try:
                value_code = get_project_internal_id_from_wikiencoding(value, session)
            except NoSuchWikiError:
                # if the wiki is new
                raise
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
    internal_id = project_q.scalar()
    if internal_id is None:
        raise NoSuchWikiError(f'Probably a new Wiki was added: {wikiencoding}')
    return project_q.scalar()


def get_project_wikiencoding_from_id(session, internal_project_id=None):
    project_q = session.query(project.id, project.code)
    if internal_project_id:
        project_q = project_q.filter(project.id==internal_project_id)
        return project_q.scalar()
    else: #getting all of these
        return project_q.all()


def get_latest_fill_id(session):
    latest_q = session.query(func.max(fill.date)).subquery()
    q = session.query(fill.id, fill.date).filter(fill.date == latest_q).filter(fill.detail['active']==True)
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

    def __init__(self, bias, props, session=None, create_if_no_exist=True):
        self.bias = bias
        self.props = hs_utils.order_props(props)
        self.props = list(props) if isinstance(props, tuple) else props
        self.all_props = [bias] + self.props
        self.props_values = [p.value for p in self.all_props]
        # not sure if this should be in the init fn
        self.session = session if session else session_factory()
        self._lookup_dict = {}
        self.create_if_no_exist = create_if_no_exist

    def build_and_execute_all_aggregations_query(self):
        # need as n+1 many aliased versions of m_a_n as there are properties including gender
        # the +1 comes from the fact that we need to verify that this is unique combination of ids
        # build the query with an accumulator pattern to make something looking like
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
            is_last_table = i == num_mans - 1

            join_type = 'join' if not is_last_table else 'outerjoin'
            # ie. a2.id = a1.id
            default_join = a_mans[i - 1].id == a_man.id
            last_join = and_(default_join, ~a_man.property.in_(self.props_values))
            join_statement = default_join if not is_last_table else last_join
            where_statement = a_man.property == self.props_values[i] if not is_last_table else a_man.id == None
            # add the join statement if it's not the first table
            if not is_first_table:
                agg_q = getattr(agg_q, join_type)(a_man, join_statement)
            # always add the where statement
            agg_q = agg_q.filter(where_statement)

        known_aggregations = agg_q.all()
        self.known_aggregations = known_aggregations
        return known_aggregations

    def build_all_aggregations_map(self, known_aggregations):
        ## Note there may be some optimizations beyond dataframe
        for wide_row in known_aggregations:
            metric_aggregations_id, vals = self._get_man_row_parts(wide_row)
            self._lookup_dict[vals] = metric_aggregations_id

    def _get_man_row_parts(self, wide_row):
        """
        In comes a 'wide_row' which is a single row which consists of many metrics_aggregations_n columns. They should all share the same id. And the last should be null.
        :param wide_row:
        :return: nested dict {'val1':{'val2':{'valn':metric_aggregation_id}}}
        """
        assert wide_row[-1] is None # artifact of SQL unique query
        wide_row = wide_row[:-1]
        vals = tuple([man_obj.value for man_obj in wide_row])
        metric_aggregations_id = wide_row[0].id
        return metric_aggregations_id, vals

        # CODE for nested dict instead of tuple as key
        # reversed_wide_row = list(reversed(wide_row))
        # assert reversed_wide_row[0] is None
        # reversed_wide_row = reversed_wide_row[1:]
        # metric_aggregations_id = reversed_wide_row[1].id
        # build_up = None
        # for man_obj in reversed_wide_row:
        #     prop_val = man_obj.value
        #     if build_up is None: # this indicates our first iteration through
        #         build_up = {prop_val: metric_aggregations_id}
        #     else:
        #         build_up = {prop_val: build_up}
        # return build_up

    def build_project_wikiencoding(self):
        project_code_id = get_project_wikiencoding_from_id(session=self.session, internal_project_id=None)
        self.intenralcode_projcode = {p.id: p.code for p in project_code_id}

    def convert_project_ids_to_wikicodes(self):
        if hs_utils.Properties.PROJECT in self.props:
            self.build_project_wikiencoding()
            proj_position = self.all_props.index(hs_utils.Properties.PROJECT)
            coded_tuples_to_add = {} #since can't add to dict as iterating over it
            for agg_tuple, agg_id in self._lookup_dict.items():
                coded_agg_tuple = tuple([e if i!= proj_position else self.intenralcode_projcode[e] for i, e in enumerate(agg_tuple)])
                coded_tuples_to_add[coded_agg_tuple] = agg_id
            # now add them back in
            for coded_agg_tuple, agg_id in coded_tuples_to_add.items():
                self._lookup_dict[coded_agg_tuple] = agg_id
        else:# Nothing to do.
            pass

    def lookup(self, bias_value, dimension_values):
        full_lookup = {**bias_value, **dimension_values}
        lookup_path = tuple(full_lookup.values())
        try:
            return self._lookup_dict[lookup_path]
        except KeyError as ke:
            if self.create_if_no_exist:
                try:
                    newly_created_obj = create_aggregations_obj(bias_value=bias_value, dimension_aggregations=dimension_values, session=self.session)
                    return newly_created_obj.id
                except NoSuchWikiError:
                    # sometimes happens is a wikidoesn't exist
                    raise
            else:
                raise KeyError(ke)

    def get_all_known_aggregations_of_props(self):
        known_aggregations = self.build_and_execute_all_aggregations_query()
        self.build_all_aggregations_map(known_aggregations) #side effect, lookup_dict is ready
        self.convert_project_ids_to_wikicodes()


class NoSuchWikiError(Exception):
    pass

class NoGenderError(Exception):
    pass
