import json
import os
import pandas as pd
import time

import pytest
from sqlalchemy import func

from humaniki_schema import generate_example_data, db
from humaniki_schema.generate_example_data import create_proj_cit_metrics
from humaniki_schema.generate_insert import insert_data
from humaniki_schema.generate_metrics import MetricCreator, MetricFactory
from humaniki_schema.queries import get_aggregations_obj, get_latest_fill_id, get_properties_obj
from humaniki_schema.schema import metric, metric_aggregations_n, project, human_country, metric_aggregations_j
from humaniki_schema.utils import read_config_file, Properties, PopulationDefinition

config = read_config_file(os.environ['HUMANIKI_YAML_CONFIG'], __file__)
session = db.session_factory()


def insert_or_skip(config, session):
    skip_insert = config['test']['skip_insert'] if 'skip_insert' in config['test'] else False
    if not skip_insert:
        data_dir = config['generation']['example']['datadir']
        num_fills = config['generation']['example']['fills']
        example_len = config['generation']['example']['len']
        curr_fill_id = insert_data(data_dir=data_dir, num_fills=num_fills, example_len=example_len)
        metrics_count = session.query(func.count(metric.fill_id)).scalar()
        print(f'number of metrics: {metrics_count}')
        # we want no metrics, a clean slate if we are inserting
        assert metrics_count == 0
    else:
        # we'll still need the curr_fill otherwise
        curr_fill_id, curr_fill_dt = get_latest_fill_id(session)
    return curr_fill_id


curr_fill_id = insert_or_skip(config, session)
print(f'curr fill id is: {curr_fill_id}')


@pytest.fixture
def test_csvs():
    test_files = {}
    test_datadir = config['test']['test_datadir']
    files = os.listdir(test_datadir)
    csv_fs = [f for f in files if f.endswith('.csv')]
    for csv_f in csv_fs:
        df = pd.read_csv(open(os.path.join(test_datadir, csv_f)))
        test_files[csv_f] = df
    return test_files

@pytest.fixture
def metric_factory():
    mf = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'])
    return mf


def test_two_dim_proj_lang_gen(test_csvs, metric_factory):
    # test that the number of metrics for the proj-citizenship prop id
    # are equal to the cardinality of the cross-product of [proj x citizenship]
    # 0. theoretical cardinality
    num_projects = session.query(func.count(project.id)).scalar()
    num_citizenships = session.query(func.count(func.distinct(human_country.country))).scalar()
    two_dim_cardinality = num_projects * num_citizenships

    # 1. first generate the project x citizenship metric
    session.query(metric).delete(); session.commit()
    num_metric_aggs = session.query()

    session.query(metric_aggregations_j).delete(); session.commit()
    session.query(metric_aggregations_n).delete(); session.commit()

    # old way:
    # proj_cit_metrics = create_proj_cit_metrics(curr_fill_id)
    mc = MetricCreator(population_definition=PopulationDefinition.GTE_ONE_SITELINK,
                       bias_property=Properties.GENDER,
                       dimension_properties=[Properties.PROJECT, Properties.CITIZENSHIP],
                       fill_id=metric_factory.curr_fill,
                       thresholds=None,
                       db_session=metric_factory.db_session)
    mc.run()

    bias_property = Properties.GENDER.value
    dimension_properties = [Properties.PROJECT.value, Properties.CITIZENSHIP.value] # note this is sorte
    proj_lang_prop = get_properties_obj(bias_property=bias_property, dimension_properties=dimension_properties, session=session)

    actual_metrics = session.query(metric).filter(metric.properties_id==proj_lang_prop.id).all()
    dimension_values = {dim_prop: None for dim_prop in dimension_properties}
    actual_aggs = get_aggregations_obj(bias_value=None, dimension_values=dimension_values, table=metric_aggregations_n, session=session)

    # 2. then check it's correct vs CSV
    expected_metrics = test_csvs['10_humans_proj_cit_metrics.csv']
    expected_aggs = test_csvs['10_humans_proj_cit_metric_aggregations_n.csv']

    assert len(actual_metrics) == len(expected_metrics)
    assert len(actual_aggs) == len(expected_aggs)

    # the two dim cardinality is the theoretical maximum
    assert len(actual_metrics) <= two_dim_cardinality

    # test that the props in the aggs are the right numbers in the right order
    first_agg_id = actual_aggs[0].id
    aggs_with_first_id = [agg for agg in actual_aggs if agg.id == first_agg_id]
    sorted_agg_group = sorted(aggs_with_first_id, key=lambda agg: agg.aggregation_order)
    assert sorted_agg_group[0].property == Properties.GENDER.value
    assert sorted_agg_group[1].property == Properties.PROJECT.value
    assert sorted_agg_group[2].property == Properties.CITIZENSHIP.value


def test_single_dim_proj_gen(test_csvs, metric_factory):
    num_projects = session.query(func.count(project.id)).scalar()

    session.query(metric).delete(); session.commit()
    session.query(metric_aggregations_j).delete(); session.commit()
    session.query(metric_aggregations_n).delete(); session.commit()

    mc = MetricCreator(population_definition=PopulationDefinition.GTE_ONE_SITELINK,
                       bias_property=Properties.GENDER,
                       dimension_properties=[Properties.PROJECT],
                       fill_id=metric_factory.curr_fill,
                       thresholds=None,
                       db_session=metric_factory.db_session)
    mc.run()

    bias_property = Properties.GENDER.value
    dimension_properties = [Properties.PROJECT.value] # note this is sorted
    proj_lang_prop = get_properties_obj(bias_property=bias_property, dimension_properties=dimension_properties, session=session)

    actual_metrics = session.query(metric).filter(metric.properties_id==proj_lang_prop.id).all()
    dimension_values = {dim_prop: None for dim_prop in dimension_properties}
    actual_aggs = get_aggregations_obj(bias_value=None, dimension_values=dimension_values, table=metric_aggregations_n, session=session)

    # 2. then check it's correct vs CSV
    expected_metrics = test_csvs['10_humans_proj_metrics.csv']
    expected_aggs = test_csvs['10_humans_proj_metric_aggregations_n.csv']

    assert len(actual_metrics) == len(expected_metrics)
    assert len(actual_aggs) == len(expected_aggs)

    # the two dim cardinality is the theoretical maximum
    assert len(actual_metrics) <= num_projects

    # test that the props in the aggs are the right numbers in the right order
    first_agg_id = actual_aggs[0].id
    aggs_with_first_id = [agg for agg in actual_aggs if agg.id == first_agg_id]
    sorted_agg_group = sorted(aggs_with_first_id, key=lambda agg: agg.aggregation_order)
    assert sorted_agg_group[0].property == Properties.GENDER.value
    assert sorted_agg_group[1].property == Properties.PROJECT.value
