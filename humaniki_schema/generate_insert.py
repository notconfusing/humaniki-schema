import datetime
import os
import sys

from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label_misc, label, metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage,project
import humaniki_schema.utils as hs_utils

try:
    import pandas as pd
    import numpy as np
except ImportError:
    raise ImportError('For this script at least we need pandas')

from humaniki_schema.db import session_factory

db_session = session_factory()

# order is important becuse of foreign key constraint
all_tables = [human_country, human_occupation, human_property, human_sitelink, label, label_misc,
              metric_aggregations_j,
              metric_properties_j,
              metric_aggregations_n,
              metric_properties_n,
              metric_coverage,
              metric,
              human,
              fill,
              project]

def clear_tables(tables):
    for table in tables:
        db_session.query(table).delete()
        db_session.commit()

def clear_misc_tables():
    for table in [project, label_misc]:
        db_session.query(table).delete()
        db_session.commit()


def make_fills(n=2):
    fills = []
    for i in range(n):
        date = datetime.date(2018, 1, 1) + datetime.timedelta(weeks=i)
        ftype = hs_utils.FillType.DUMP.value
        detail = {'active': True}
        a_fill = fill(date=date, type=ftype, detail=detail)
        fills.append(a_fill)

    db_session.rollback()
    db_session.add_all(fills)
    db_session.commit()
    return fills


def make_humans(example_len, data_dir, fills, fake_by_subset=True):
    humans_f = os.path.join(data_dir, f'denelezh_humans_{example_len}.tsv')
    humans_df = pd.read_csv(humans_f, sep='\t').rename(columns={"birthyear": 'year_of_birth'})
    humans_df['year_of_death'] = humans_df['year_of_birth'].apply(lambda yob: yob + 100 if yob is not None else None)
    humans_df = humans_df.replace(dict(year_of_birth={np.nan: None}, year_of_death={np.nan: None}))
    len_humans_df = len(humans_df)
    half_length_human_df = int(len_humans_df/2)
    humans = []

    first_fill_id = fills[0].id
    for fill in fills:
        fill_id = fill.id
        is_first_fill = fill_id == first_fill_id
        for ind, row in humans_df.iterrows():
            # make two kind of distinct subsets by setting the first one to be only half the length
            if fake_by_subset and is_first_fill and ind > half_length_human_df:
                print(f'faking by skipping')
                continue
            else:
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


def make_table_from_file(fname, schema_table, table_tsv_map, data_dir, fills, extra_const_cols=None):
    table_f = os.path.join(data_dir, fname)
    table_df = pd.read_csv(table_f, sep='\t')
    if extra_const_cols:
        for col, const in extra_const_cols.items():
            table_df[col] = const

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


def make_projects(data_dir):
    """seperate because project data needs to be migrated first-setup scenarios"""
    projects = make_table_exactly_from_file(fname='denelezh_project.tsv',
                                            schema_table=project,
                                            table_tsv_map={'type': 'type', 'code': 'code', 'label': 'label',
                                                           'url': 'url'},
                                            data_dir=data_dir,
                                            fills=None)
    return projects

def make_label_misc_project(data_dir):
    """seperate because project data needs to be migrated first-setup scenarios"""
    labels_misc = make_table_exactly_from_file(fname=f'denelezh_project.tsv',
                                       schema_table=label_misc,
                                       table_tsv_map={'src': 'code', 'label': 'label', 'lang':'lang', 'type':'type'},
                                       data_dir=data_dir,
                                       fills=None,
                                       extra_const_cols={'lang':'en', 'type':'project'})
    return labels_misc

def make_label_misc_gender_labels(data_dir):
    bias_labels = make_table_exactly_from_file(fname=f'denelezh_label_biases.tsv',
                                       schema_table=label_misc,
                                       table_tsv_map={'src': 'id', 'lang': 'lang', 'label': 'label', 'type':'type'},
                                       data_dir=data_dir,
                                       fills=None)
    print(f'inserted: {len(bias_labels)} bias_labels')
    return bias_labels


def make_country_misc_iso(data_dir):
    country_misc_iso = make_table_exactly_from_file(fname='wdqs_country_labels_iso.tsv',
                                 schema_table=label_misc,
                                 table_tsv_map={'src': 'id', 'label': 'label',
                                                'lang': 'lang', 'type': 'type'},
                                 data_dir=data_dir,
                                 fills=None,
                                 extra_const_cols={'type': 'iso_3166'})
    return country_misc_iso

def make_table_exactly_from_file(fname, schema_table, table_tsv_map, data_dir, fills, extra_const_cols=None):
    table_f = os.path.join(data_dir, fname)
    table_df = pd.read_csv(table_f, sep='\t').replace(dict(type={np.nan: None}))
    if extra_const_cols:
        for col, const in extra_const_cols.items():
            table_df[col] = const
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


def insert_data(data_dir='example_data', example_len=500, num_fills=2):
    clear_tables(all_tables)
    fills = make_fills(num_fills)

    print(f'inserted: {len(fills)} fills')
    just_latest_fill = [fills[num_fills - 1]]
    curr_fill = just_latest_fill[0].id

    humans = make_humans(example_len, data_dir, fills)
    print(f'inserted: {len(humans)} humans')

    countries = make_table_from_file(fname=f'denelezh_human_country_{example_len}.tsv',
                                     schema_table=human_country,
                                     table_tsv_map={'human_id': 'human', 'country': 'country'},
                                     data_dir=data_dir,
                                     fills=fills)
    print(f'inserted: {len(countries)} countries')

    occupations = make_table_from_file(fname=f'denelezh_human_occupation_{example_len}.tsv',
                                       schema_table=human_occupation,
                                       table_tsv_map={'human_id': 'human', 'occupation': 'occupation'},
                                       data_dir=data_dir,
                                       fills=fills)
    print(f'inserted: {len(occupations)} occupations')

    sitelinks = make_table_from_file(fname=f'denelezh_human_sitelink_{example_len}.tsv',
                                     schema_table=human_sitelink,
                                     table_tsv_map={'human_id': 'human', 'sitelink': 'sitelink'},
                                     data_dir=data_dir,
                                     fills=fills)
    print(f'inserted: {len(sitelinks)} sitelinks')

    labels = make_table_from_file(fname=f'denelezh_label_{example_len}.tsv',
                                  schema_table=label,
                                  table_tsv_map={'qid': 'id', 'lang': 'lang', 'label': 'label'},
                                  data_dir=data_dir,
                                  fills=just_latest_fill)
    print(f'inserted: {len(labels)} labels')

    label_miscs = make_table_exactly_from_file(fname=f'denelezh_project.tsv',
                                       schema_table=label_misc,
                                       table_tsv_map={'src': 'code', 'label': 'label', 'lang':'lang', 'type':'type'},
                                       data_dir=data_dir,
                                       fills=None,
                                       extra_const_cols={'lang':'en', 'type':'project'})
    print(f'inserted: {len(label_miscs)} labels_misc')

    labels_misc_fr = make_table_exactly_from_file(fname=f'denelezh_project_frfake.tsv',
                                       schema_table=label_misc,
                                       table_tsv_map={'src': 'code', 'label': 'label', 'lang':'lang', 'type':'type'},
                                       data_dir=data_dir,
                                       fills=None,
                                       extra_const_cols={'lang':'fr', 'type':'project'})
    print(f'inserted: {len(labels_misc_fr)} labels_misc fake french')
    country_fr = make_table_exactly_from_file(fname='wdqs_country_labels_fr.tsv',
                                            schema_table=label,
                                            table_tsv_map={'qid': 'id', 'label': 'label',
                                                           'lang': 'lang'},
                                            data_dir=data_dir,
                                            fills=None)
    print(f'inserted: {len(country_fr)} country_fr')
    country_en = make_table_exactly_from_file(fname='wdqs_country_labels_en.tsv',
                                            schema_table=label,
                                            table_tsv_map={'qid': 'id', 'label': 'label',
                                                           'lang': 'lang'},
                                            data_dir=data_dir,
                                            fills=None)
    print(f'inserted: {len(country_en)} country_en')
    country_iso = make_table_exactly_from_file(fname='wdqs_country_labels_iso.tsv',
                                            schema_table=label,
                                            table_tsv_map={'qid': 'id', 'label': 'label',
                                                           'lang': 'lang'},
                                            data_dir=data_dir,
                                            fills=None)
    print(f'inserted: {len(country_iso)} country_iso')

    #### Static ones that are needed
    country_misc_iso = make_country_misc_iso(data_dir=data_dir)
    print(f'inserted: {len(country_misc_iso)} country_misc_iso')

    projects = make_projects(data_dir=data_dir)
    print(f'inserted: {len(projects)} projects')

    label_miscs = make_label_misc_project(data_dir=data_dir)
    print(f'inserted: {len(label_miscs)} label misc')

    label_miscs = make_label_misc_gender_labels(data_dir=data_dir)
    print(f'inserted: {len(label_miscs)} label misc')

    return curr_fill



if __name__ == "__main__":
    fn_name = sys.argv[1] if len(sys.argv) >= 2 else None
    data_dir = 'example_data'
    if fn_name is None:
        clear_misc_tables()
        make_projects(data_dir=data_dir)
        make_country_misc_iso(data_dir=data_dir)
        make_label_misc_project(data_dir=data_dir)
        make_label_misc_gender_labels(data_dir=data_dir)
    else:
        locals()[fn_name](data_dir=data_dir)
