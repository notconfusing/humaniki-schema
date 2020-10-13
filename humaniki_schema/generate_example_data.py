import sys

from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.orm import sessionmaker
import datetime
import json
import os
# from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
#                                     metric, metric_aggregations, metric_coverage, metric_properties
from humaniki_schema.generate_insert import insert_data
from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
    metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    project
import humaniki_schema.utils as hs_utils

try:
    import pandas as pd
    import numpy as np
except ImportError:
    raise ImportError('For this script at least we need pandas')

data_dir = 'example_data'

from humaniki_schema.db import session_factory
from humaniki_schema.db import engine as db_engine

db_session = session_factory()

# ## metrics
# 1. geographic metric
# 1. by language
# 2. multi - language / geography
# 3. multi - lanaguge / geography / occupation

def get_or_create_agg_vals(bias_value, agg_vals):
    quoted_agg_vals = [f"'{val}'" if isinstance(val, str) else val for val in agg_vals]
    agg_equals_parts = [f"and json_extract(aggregations, '$[{pos}]')={val}" for pos, val in enumerate(quoted_agg_vals)]

    agg_equals_sql = f'''select id, aggregations from metric_aggregations_j where
                        aggregations_len={len(agg_vals)}
                        and bias_value={bias_value}
                        {' '.join(agg_equals_parts)};
                        '''
    #     print(agg_equals_sql)
    agg_vals_rec = db_engine.execute(agg_equals_sql).fetchall()
    #     print(agg_vals_rec)
    if not agg_vals_rec:
        #         print(agg_vals , 'not found')
        a_metric_aggregation = metric_aggregations_j(aggregations=agg_vals,
                                                     aggregations_len=len(agg_vals),
                                                     bias_value=bias_value)
        db_session.rollback()
        db_session.add(a_metric_aggregation)
        db_session.commit()
        return a_metric_aggregation.id
    else:
        assert len(agg_vals_rec) == 1
        return agg_vals_rec[0][0] #the first column of the first row, hopefully the id


def get_or_create_metric_props(bias_property, metric_props):
    #     metric_props_rec = db_session.query(metric_properties).filter_by(properties=metric_props).one_or_none()
    metric_equals_parts = [f"and json_extract(properties, '$[{pos}]')={val}" for pos, val in enumerate(metric_props)]

    metric_equals_sql = f'''select id, properties from metric_properties_j where
                        properties_len={len(metric_props)}
                        and bias_property={bias_property}
                        {' '.join(metric_equals_parts)}
                        ;
                        '''
    #     print(metric_equals_sql)
    metric_props_rec = db_engine.execute(metric_equals_sql).fetchall()
    if not metric_props_rec:
        #         print(metric_props)
        a_metric_properties = metric_properties_j(properties=metric_props,
                                                    properties_len=len(metric_props),
                                                  bias_property=bias_property)
        db_session.rollback()
        db_session.add(a_metric_properties)
        db_session.commit()
        return a_metric_properties.id
    else:
        assert len(metric_props_rec) == 1
        return metric_props_rec[0][0] #the first column of the first row, hopefully the id

# def generate_geo_metrics():
#     geo_metric_q = db_session.query(human.gender, human_country.country, func.count(human.gender))     .join(human_country, and_(human.qid==human_country.human_id, human.fill_id==human_country.fill_id))    .filter(human.fill_id==curr_fill)    .group_by(human_country.country, human.gender)
#
#     geo_metric_res = geo_metric_q.all()
#
#     print(str(geo_metric_q))
#
# #     geo_metric_q = db_session.query(human.gender, human_country.country, func.count(human.gender)) \
# #     .join(human_country).filter(human.fill_id==curr_fill)\
# #     .group_by(human_country.country, human.gender)
#
# #     geo_metric_res = geo_metric_q.all()
#     return geo_metric_res
# geo_metric_res = generate_geo_metrics()
#
#
# # In[24]:
#
#
# def insert_geo_metrics():
#     geo_metrics = []
#     for gender, country, count in geo_metric_res:
#         agg_vals_id = get_or_create_agg_vals([gender, country])
#         m_props_id = get_or_create_metric_props([-1])
#     #     db_session.commit()
#         fills_id = curr_fill
#         db_session.rollback()
#         a_metric = metric(fill_id=fills_id,
# #                          facet='geography',
#                          population_id=hs_utils.PopulationDefinition.ALL_WIKIDATA.value,
#                          properties_id=m_props_id,
#                          aggregations_id=agg_vals_id,
#                          bias_value=gender,
#                          total=count)
# #         print(a_metric)
#         geo_metrics.append(a_metric)
# #     db_session.add_all(geo_metrics)
# #     db_session.commit()
#     return geo_metrics
# geo_metrics = insert_geo_metrics()
# db_session.add_all(geo_metrics)
# db_session.commit()


# def generate_single_facet_metric(agg_table, agg_table_col):
#     query_columns = human.gender, agg_table_col, func.count(human.gender)
#     query_columns_str = [str(c) for c in query_columns]
#     metric_q = db_session.query(*query_columns)     .join(agg_table, and_(human.qid==agg_table.human_id, human.fill_id==agg_table.fill_id))    .filter(human.fill_id==curr_fill)    .group_by(agg_table_col, human.gender)
#
#     metric_res = metric_q.all()
#
# #     print(str(metric_q))
#     return query_columns_str, metric_res

# proj_metric_strs, proj_metric_res = generate_single_facet_metric(human_sitelink, human_sitelink.sitelink)



def create_sitelink_metrics(curr_fill):
    sitelink_metric_q = db_session.query(human.gender, human_sitelink.sitelink, func.count(human.gender)).join(
        human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)).join(project,
                                                                                                                  human_sitelink.sitelink == project.code).filter(
        human.fill_id == curr_fill).filter(project.type == 'wikipedia').group_by(human_sitelink.sitelink, human.gender)

    print('making metrics')
    proj_metric_res = sitelink_metric_q.all()
    print(f'made {len(proj_metric_res)} sitelink metrics')
    metrics = insert_single_prop_metrics(bias=hs_utils.Properties.GENDER, prop=hs_utils.Properties.PROJECT,
                                         metric_rows=proj_metric_res)

    db_session.add_all(metrics)
    db_session.commit()

def insert_single_prop_metrics(bias, prop, metric_rows):
    sf_metrics = []
    for gender, prop_val, count in metric_rows:
        agg_vals_id = get_or_create_agg_vals(gender, [prop_val])
        props_pid = prop.value
        m_props_id = get_or_create_metric_props(bias.value, [props_pid])
        fills_id = curr_fill
        #         db_session.rollback()
        a_metric = metric(fill_id=fills_id,
                          population_id=hs_utils.PopulationDefinition.GTE_ONE_SITELINK.value,
                          properties_id=m_props_id,
                          aggregations_id=agg_vals_id,
                          bias_value=gender,
                          total=count)
        sf_metrics.append(a_metric)

    return sf_metrics


if __name__ == '__main__':
    data_dir = os.getenv('HUMANIKI_EXMAPLE_DATADIR', 'example_data')
    num_fills = os.getenv('HUMANIKI_EXAMPLE_FILLS', 2)
    example_len = os.getenv('HUMANIKI_EXAMPLE_LEN', 10)

    curr_fill = insert_data(data_dir=data_dir, num_fills=num_fills, example_len=example_len)

    create_sitelink_metrics(curr_fill)
