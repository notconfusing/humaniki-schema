import sqlalchemy
import sys
import time
from sqlalchemy import create_engine, func, and_, or_
import datetime
import json
import os

from sqlalchemy.orm import aliased

from humaniki_schema.generate_insert import insert_data
from humaniki_schema.queries import get_properties_obj, get_aggregations_obj, get_latest_fill_id, AggregationIdGetter
from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
    metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    project
from humaniki_schema.db import session_factory
from humaniki_schema.db import engine as db_engine
import humaniki_schema.utils as hs_utils
import pandas as pd
import numpy as np

db_session = session_factory()


def create_dob_metrics(curr_fill):
    # details for abstraction
    # dimension column: in this case its also in humans and doesn't require a join
    # checking that the dimension is not null
    # then then seperately the property we're using (which is isomorphic data)
    dob_metric_q = db_session.query(human.gender, human.year_of_birth, func.count(human.gender)) \
        .filter(human.year_of_birth.isnot(None)) \
        .filter(human.fill_id == curr_fill) \
        .group_by(human.year_of_birth, human.gender)

    print('making metrics')
    dob_metric_res = dob_metric_q.all()
    print(f'made {len(dob_metric_res)} dob metrics')
    metrics = insert_metrics(bias=hs_utils.Properties.GENDER, prop=hs_utils.Properties.DATE_OF_BIRTH,
                             metric_rows=dob_metric_res, curr_fill=curr_fill)
    return metrics


def create_sitelink_metrics(curr_fill):
    # this requires a third join to filter just for wikipedia types (but not sure its desirable long term)
    sitelink_metric_q = db_session.query(human.gender, human_sitelink.sitelink, func.count(human.gender)).join(
        human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)).join(
        project,
        human_sitelink.sitelink == project.code).filter(
        human.fill_id == curr_fill).filter(project.type == 'wikipedia').group_by(human_sitelink.sitelink, human.gender)

    print('making metrics')
    proj_metric_res = sitelink_metric_q.all()
    print(f'made {len(proj_metric_res)} sitelink metrics')
    metrics = insert_metrics(bias=hs_utils.Properties.GENDER, prop=hs_utils.Properties.PROJECT,
                             metric_rows=proj_metric_res, curr_fill=curr_fill)
    return metrics


def create_geo_metrics(curr_fill):
    metric_q = db_session.query(human.gender, human_country.country, func.count(human.gender)) \
        .join(human_country, and_(human.qid == human_country.human_id, human.fill_id == human_country.fill_id)) \
        .group_by(human_country.country, human.gender)

    print('making geo metrics')
    metric_res = metric_q.all()
    print(f'made {len(metric_res)} geo metrics')
    metrics = insert_metrics(bias=hs_utils.Properties.GENDER, prop=hs_utils.Properties.CITIZENSHIP,
                             metric_rows=metric_res, curr_fill=curr_fill)
    return metrics

def create_proj_cit_metrics(curr_fill):
    # TODO need to ensure that these are in the right order
    metric_q = db_session.query(human.gender, human_sitelink.sitelink, human_country.country, func.count(human.gender)) \
        .join(human_country, and_(human.qid == human_country.human_id, human.fill_id == human_country.fill_id)) \
        .join(human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)) \
        .group_by(human_country.country, human_sitelink.sitelink, human.gender)

    print('making proj-cit metrics')
    metric_res = metric_q.all()
    print(f'made {len(metric_res)} proj-cit metrics')
    metrics = insert_metrics(bias=hs_utils.Properties.GENDER, props=[hs_utils.Properties.PROJECT, hs_utils.Properties.CITIZENSHIP],
                             metric_rows=metric_res, curr_fill=curr_fill)
    return metrics


def get_agg_vals_id(bias_value, dimension_values):
    """
    get the aggregation id based on the aggregation values.
    the simple way is to check each aggregation-combination, and do create-if-no-exist,
    however that requires many roundtrips to the database and is slow. we will need optimize
    by keeping the table in memory. props
    :return: scalar aggregations_id
    """
    agg_vals_obj = get_aggregations_obj(bias_value=bias_value, dimension_values=dimension_values,
                                        session=db_session, create_if_no_exist=True)
    # if the agg_vals_obj was created we get it back, otherwise, we get a list of results, which should just contain
    # one result from json row, but we need to use .all() to maintain backend-end getting agg_vals_objs
    if isinstance(agg_vals_obj, list):
        assert len(agg_vals_obj) == 1
        agg_vals_obj = agg_vals_obj[0]
    agg_vals_id = agg_vals_obj.id
    return agg_vals_id

def insert_metrics(bias, props, metric_rows, curr_fill, population_id=None):
    sf_metrics = []
    aggregation_props_gathering_start = time.time()
    aggregation_getter = AggregationIdGetter(bias=bias, props=props)
    aggregation_getter.get_all_known_aggregations_of_props()
    # these remain static
    prop_pids = [p.value for p in props] if isinstance(props, list) else [props.value] #backwards compatible for single dimension metrics
    m_props = get_properties_obj(bias_property=bias.value, dimension_properties=prop_pids, session=db_session, create_if_no_exist=True)
    m_props_id = m_props.id
    fills_id = curr_fill
    population_id = hs_utils.PopulationDefinition.GTE_ONE_SITELINK.value if population_id is None else population_id


    for row_i, row in enumerate(metric_rows):
        # TODO do this by name lookup not positions
        if row_i%100==0:
            print(row_i)
        gender = row[0]
        count = row[-1]
        prop_vals = row[1:-1]
        dimension_values = {prop_id: prop_val for prop_id, prop_val in zip(prop_pids, prop_vals)}
        agg_vals_id = get_agg_vals_id(bias_value={bias.value:gender},
                                      dimension_values=dimension_values)
        a_metric = metric(fill_id=fills_id,
                          population_id=population_id,
                          properties_id=m_props_id,
                          aggregations_id=agg_vals_id,
                          bias_value=gender,
                          total=count)
        sf_metrics.append(a_metric)

    aggregation_props_gathering_end = time.time()
    insertion_start = time.time()
    # try:
    #     db_session.bulk_save_objects(sf_metrics)
    # except sqlalchemy.exc.IntegrityError as insert_error:
    #     if insert_error.orig.args[1].startswith('Duplicate entry'):
    #         print('attempting to add a metric thats already been added')
    #         db_session.rollback()
    db_session.add_all(sf_metrics)
    db_session.commit()
    insertion_end = time.time()
    print(f'aggregation_props lookup took {aggregation_props_gathering_end-aggregation_props_gathering_start} seconds')
    print(f'inserting objects took {insertion_end-insertion_start} seconds')
    return sf_metrics


def generate_all(config=None):
    start_time = time.time()

    if config is None:
        config = hs_utils.read_config_file(os.environ['HUMANIKI_YAML_CONFIG'], __file__)
    data_dir = config['generation']['example']['datadir']
    num_fills = config['generation']['example']['fills']
    example_len = config['generation']['example']['len']
    skip_steps = config['generation']['skip_steps'] if 'skip_steps' in config['generation'] else []

    if 'insert' not in skip_steps:
        curr_fill = insert_data(data_dir=data_dir, num_fills=num_fills, example_len=example_len)
    else:
        curr_fill, curr_fill_date = get_latest_fill_id(db_session)

    if 'geo' not in skip_steps:
        geom = create_geo_metrics(curr_fill)
        print(f'created geo metrics that have len {len(geom)}')
        end_time = time.time()
        print(f'Generating data took {end_time-start_time} seconds')

    if 'sitelinks' not in skip_steps:
        slm = create_sitelink_metrics(curr_fill)
        print(f'created sitelink metrics that have len {len(slm)}')
        end_time = time.time()
        print(f'Generating data took {end_time-start_time} seconds')

    if 'dob' not in skip_steps:
        dobm = create_dob_metrics(curr_fill)
        print(f'created date of birth metrics that have len {len(dobm)}')
        end_time = time.time()
        print(f'Generating data took {end_time-start_time} seconds')

    if 'proj_cit' not in skip_steps:
        proj_cit_m = create_proj_cit_metrics(curr_fill)
        print(f'created date of projxcit metrics that have len {len(proj_cit_m)}')

    db_session.commit()
    return True


if __name__ == '__main__':
    generate_all()
