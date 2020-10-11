import sys

from sqlalchemy import create_engine, func, and_, or_
from sqlalchemy.orm import sessionmaker
import datetime
import json
import os
# from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
#                                     metric, metric_aggregations, metric_coverage, metric_properties
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

# order is important becuse of foreign key constraint
all_tables = [human_country, human_occupation, human_property, human_sitelink, label,
              metric_aggregations_j,
              metric_properties_j,
              metric_aggregations_n,
              metric_properties_n,
              metric_coverage,
              metric,
              human,
              fill,
              project]

for table in all_tables:
    db_session.query(table).delete()
    db_session.commit()


# In[6]:


def make_fills(n=2):
    fills = []
    for i in range(n):
        date = datetime.date(2018, 1, 1) + datetime.timedelta(weeks=i)
        ftype = hs_utils.FillType.DUMP.value
        detail = {'i': i, 'i_str': str(i)}
        a_fill = fill(date=date, type=ftype, detail=detail)
        fills.append(a_fill)

    db_session.rollback()
    db_session.add_all(fills)
    db_session.commit()
    return fills

num_fills = 2
fills = make_fills(num_fills)
curr_fill = fills[num_fills-1].id

# example_len = 500# you must have created these dataset
# example_len = 10# you must have created these dataset
example_len = os.getenv("example_data_len")
# print(f"example_len is{example_len}")
def make_humans():
    humans_f = os.path.join(data_dir, f'denelezh_humans_{example_len}.tsv')
    humans_df = pd.read_csv(humans_f, sep='\t').rename(columns={"birthyear": 'year_of_birth'})
    humans_df['year_of_death'] = humans_df['year_of_birth'].apply(lambda yob: yob + 100 if yob is not None else None)
    humans_df = humans_df.replace(dict(year_of_birth={np.nan: None}, year_of_death={np.nan: None}))
    humans = []

    for fill in fills:
        fill_id = fill.id
        for ind, row in humans_df.iterrows():
            a_human = human(fill_id=fill_id, qid=row['id'],
                            year_of_birth=row['year_of_birth'],
                            year_of_death=row['year_of_death'],
                            gender=row['gender'],
                            sitelink_count=row['sitelinks'])
            humans.append(a_human)

    db_session.rollback()
    db_session.add_all(humans)
    db_session.commit()
    return humans


humans = make_humans()


# In[8]:


def make_table_from_file(fname, schema_table, table_tsv_map, include_fill_col=True):
    table_f = os.path.join(data_dir, fname)
    table_df = pd.read_csv(table_f, sep='\t')
    insert_rows = []

    for fill in fills:
        fill_id = fill.id
        for ind, row in table_df.iterrows():
            params = {'fill_id': fill_id}
            for table_name, tsv_name in table_tsv_map.items():
                params[table_name] = row[tsv_name]
            a_row = schema_table(**params)
            insert_rows.append(a_row)

    db_session.rollback()
    db_session.add_all(insert_rows)
    db_session.commit()
    return insert_rows


# In[9]:


countries = make_table_from_file(fname=f'denelezh_human_country_{example_len}.tsv',
                                 schema_table=human_country,
                                 table_tsv_map={'human_id': 'human', 'country': 'country'})

# In[10]:


occupations = make_table_from_file(fname=f'denelezh_human_occupation_{example_len}.tsv',
                                   schema_table=human_occupation,
                                   table_tsv_map={'human_id': 'human', 'occupation': 'occupation'})

# In[11]:


sitelinks = make_table_from_file(fname=f'denelezh_human_sitelink_{example_len}.tsv',
                                 schema_table=human_sitelink,
                                 table_tsv_map={'human_id': 'human', 'sitelink': 'sitelink'})

# In[12]:


labels = make_table_from_file(fname=f'denelezh_label_{example_len}.tsv',
                              schema_table=label,
                              table_tsv_map={'qid': 'id', 'lang': 'lang', 'label': 'label'})

bias_labels = make_table_from_file(fname=f'denelezh_label_biases.tsv',
                              schema_table=label,
                              table_tsv_map={'qid': 'id', 'lang': 'lang', 'label': 'label'})

def make_table_exactly_from_file(fname, schema_table, table_tsv_map):
    table_f = os.path.join(data_dir, fname)
    table_df = pd.read_csv(table_f, sep='\t').replace(dict(type={np.nan: None}))
    insert_rows = []

    for ind, row in table_df.iterrows():
        params = {}
        for table_name, tsv_name in table_tsv_map.items():
            params[table_name] = row[tsv_name]
        a_row = schema_table(**params)
        insert_rows.append(a_row)

    db_session.rollback()
    db_session.add_all(insert_rows)
    db_session.commit()
    return insert_rows


# In[15]:


projects = make_table_exactly_from_file(fname='denelezh_project.tsv',
                                        schema_table=project,
                                        table_tsv_map={'type': 'type', 'code': 'code', 'label': 'label', 'url': 'url'})


# ## metrics
# 1. geographic metric
# 1. by language
# 2. multi - language / geography
# 3. multi - lanaguge / geography / occupation

# In[16]:


def get_or_create_agg_vals(bias_value, agg_vals):
    #     agg_vals_rec = db_session.query(metric_aggregations).filter_by(aggregations=agg_vals).one_or_none()
    # select id, aggregations from metric_aggregations
    #     where json_length(aggregations)=2
    #         and json_extract(aggregations, '$[1]')=10
    #         and json_extract(aggregations, '$[0]')=6581097
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


sitelink_metric_q = db_session.query(human.gender, human_sitelink.sitelink, func.count(human.gender)).join(
    human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)).join(project,
                                                                                                              human_sitelink.sitelink == project.code).filter(
    human.fill_id == curr_fill).filter(project.type == 'wikipedia').group_by(human_sitelink.sitelink, human.gender)

print('making metrics')
proj_metric_res = sitelink_metric_q.all()
print('made sitelink metric')


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


metrics = insert_single_prop_metrics(bias=hs_utils.Properties.GENDER, prop=hs_utils.Properties.PROJECT, metric_rows=proj_metric_res)

db_session.add_all(metrics)
db_session.commit()
print("done")
