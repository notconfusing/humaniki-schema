import datetime
import json
import os
import sqlalchemy
import sys
import time
from os import listdir

from sqlalchemy.orm.attributes import flag_modified
import humaniki_schema
from humaniki_schema.queries import get_latest_fill_id
from humaniki_schema.schema import fill, human, human_country, human_occupation, human_property, human_sitelink, label, \
    metric, metric_properties_j, metric_properties_n, metric_aggregations_j, metric_aggregations_n, metric_coverage, \
    project, label_misc
import humaniki_schema.utils as hs_utils

try:
    import pandas as pd
    import numpy as np
except ImportError:
    raise ImportError('For this script at least we need pandas')

from humaniki_schema.db import session_factory


class humanikiDataInserter():
    def __init__(self, config, dump_date=None, dump_subset=None, insert_strategy=None):
        self.config = hs_utils.read_config_file(config, __file__)
        self.config_insertion = self.config['insertion']
        self.overwrite = self.config_insertion['overwrite'] if 'overwrite' in self.config_insertion else False
        self.only_files = self.config_insertion['only_files'] if 'only_files' in self.config_insertion else None
        self.insert_strategy = insert_strategy if insert_strategy is not None else "infile"
        self.dump_date = self._make_dump_date(dump_date) if dump_date else None
        self.dump_subset = dump_subset
        self.dump_date_str = None
        self.fill_id = None
        self.detection_type = None
        self.db_session = session_factory()
        # order is important becuse of foreign key constraint
        self.csvs = None
        self.CSV_NA_VALUE = r'\N'
        self.table_column_map = {
            'human':
                {"insert_columns": ['qid', 'gender', 'year_of_birth', 'sitelink_count'],
                 "extra_const_columns": {},
                 "escaping_options": ""},
            'human_country':
                {"insert_columns": ['human_id', 'country'],
                 "extra_const_columns": {},
                 "escaping_options": ""},
            'human_occupation':
                {"insert_columns": ['human_id', 'occupation'],
                 "extra_const_columns": {},
                 "escaping_options": ""},
            'human_sitelink':
                {"insert_columns": ['human_id', 'sitelink'],
                 "extra_const_columns": {},
                 "escaping_options": ""},
            'label':
                {"insert_columns": ['qid', 'label'],
                 "extra_const_columns": {'lang': 'en'},
                 "escaping_options": """OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\\\'"""},
            'occupation_parent':
                {"insert_columns": ['occupation', 'parent'],
                 "extra_const_columns": {},
                 "escaping_options": ""},
        }

    def _make_dump_date(self, datestr):
        return datetime.datetime.strptime(datestr, '%Y%m%d').date()

    def detect_fill_date(self):
        if self.dump_date is not None:
            self.detection_type = 'explicit'
        else:
            dump_dir = self.config_insertion['wdtk_processing_output']
            latest_dir = max(listdir(dump_dir))
            latest_dt = self._make_dump_date(latest_dir)
            self.dump_date = latest_dt
            self.detection_type = 'latest'
        # finally
        self.dump_date_str = self.dump_date.strftime('%Y%m%d')

    def validate_extant_csvs(self):
        self.csv_dir = os.path.join(self.config_insertion['wdtk_processing_output'], self.dump_date.strftime('%Y%m%d'))
        self.csv_dir = os.path.join(self.csv_dir, self.dump_subset) if self.dump_subset else self.csv_dir

        all_files = os.listdir(self.csv_dir)
        allowable_csvs = [f"{table_name}.csv" for table_name in self.table_column_map.keys()]
        extant_csvs = [f for f in all_files if f.endswith('.csv')]
        self.csvs = []
        for csv in extant_csvs:
            if csv not in allowable_csvs:
                print(f'Not an allowable CSV: {csv}')
            else:
                self.csvs.append(csv)
        assert len(self.csvs) == len(allowable_csvs)

    def create_fill_item(self):
        try:
            prev_latest_fill_id, prev_latest_fill_dt = get_latest_fill_id(self.db_session)
        except sqlalchemy.orm.exc.NoResultFound: # might happen if its the first time
            previous_fill_id, prev_latest_fill_dt = None, None

        # Deal with an existing dump
        if prev_latest_fill_dt and  prev_latest_fill_dt == self.dump_date:
            print(f'previous fill item found for {self.dump_date}')
            if not self.overwrite:
                raise AssertionError(f'already have a dump of this date, and overwrite is {self.overwrite}')
            else:
                old_fill = self.db_session.query(fill).filter(fill.date == self.dump_date).filter(
                    fill.detail['active'] == True).one()
                old_fill.detail['active'] = False
                flag_modified(old_fill, "detail")
                self.db_session.add(old_fill, )
                self.db_session.commit()

        # nothing exists yet
        fill_type = hs_utils.FillType.DUMP.value
        now = datetime.datetime.utcnow()
        detail = {'fill_process_dt': now.strftime(hs_utils.HUMANIKI_SNAPSHOT_DATE_FMT),
                  'detection_type': self.detection_type,
                  'extant_csvs': self.csvs, 'active': True}
        a_fill = fill(date=self.dump_date, type=fill_type, detail=detail)
        self.db_session.add(a_fill)
        self.db_session.commit()
        self.fill_id = a_fill.id

    def _persist_rows(self, rows, method='bulk'):
        if method == 'bulk':
            self.db_session.bulk_save_objects(rows)
            # except sqlalchemy.exc.IntegrityError as insert_error:
            #     if insert_error.orig.args[1].startswith('Duplicate entry'):
            #         print('attempting to add a metric thats already been added')
            #         self.db_session.rollback()
            print(f'bulk persisted {len(rows)} rows')
            self.db_session.commit()
        else:
            self.db_session.add_all(rows)
            self.db_session.commit()
            print(f'standard persisted {len(rows)} rows')
        return rows

    def _insert_csv(self, table_f, extra_const_cols, schema_table, columns):
        print(f'now processing {table_f}')
        insert_rows = []
        try:
            table_df = pd.read_csv(table_f, sep=',', header=None, index_col=False, na_values=r'\N')
            # table_df = table_df.replace()
        except pd.errors.EmptyDataError:
            return insert_rows

        table_df.columns = columns
        if extra_const_cols:
            for col, const in extra_const_cols.items():
                table_df[col] = const

        ## TODO optimize this part without iteration
        for ind, row in table_df.iterrows():
            row_params = {'fill_id': self.fill_id}
            for col, val in row.items():
                if pd.isnull(val):
                    val = None
                row_params[col] = val
            a_row = schema_table(**row_params)
            insert_rows.append(a_row)
        print(f'there are {len(insert_rows)} rows to insert for {schema_table}')
        return insert_rows

    def insert_csvs_pandas(self):
        for csv in self.csvs:
            csv_f = os.path.join(self.csv_dir, csv)
            csv_table_name = csv.split('.csv')[0]
            schema_table = getattr(humaniki_schema.schema, csv_table_name)
            extra_const_cols = self.table_const_map[
                csv_table_name] if csv_table_name in self.table_const_map.keys() else None

            # skip this file by filename
            if self.only_files is not None and csv_table_name not in self.only_files:
                print(f'Only_files acvtive, so skipping {csv_table_name}')
                continue
            insert_create_row_objs_start = time.time()
            insert_rows = self._insert_csv(table_f=csv_f,
                                           extra_const_cols=extra_const_cols,
                                           schema_table=schema_table,
                                           columns=self.table_column_map[csv_table_name])
            insert_persist_row_objs_start = time.time()
            self._persist_rows(insert_rows)
            insert_persist_row_objs_end = time.time()
            print(
                f'INSERT, creating row objects took: {insert_persist_row_objs_start-insert_create_row_objs_start} seconds')
            print(
                f'INSERT, persisting row objects took: {insert_persist_row_objs_end-insert_persist_row_objs_start} seconds')

    def execute_single_infile(self, csv_f, csv_table_name, column_insertion_order, extra_const_cols, escaping_options):
        column_list_str = ','.join(column_insertion_order)
        # TODO supports just one extra const col for now
        extra_const_str = (',' + [f"{k}='{v}'" for k, v in extra_const_cols.items()][0]) if extra_const_cols else ''
        infile_sql = f"""
        LOAD DATA INFILE '{csv_f}' IGNORE
            INTO TABLE `{csv_table_name}` FIELDS TERMINATED BY ',' {escaping_options}
                ({column_list_str})
                set fill_id={self.fill_id} {extra_const_str};
        """
        print(infile_sql)
        self.db_session.get_bind().execute(infile_sql)

    def insert_csvs_infile(self):
        for csv in self.csvs:
            csv_f = os.path.join(self.csv_dir, csv)
            csv_table_name = csv.split('.csv')[0]
            column_insertion_order = self.table_column_map[csv_table_name]['insert_columns']
            extra_const_cols = self.table_column_map[csv_table_name]['extra_const_columns']
            escaping_options = self.table_column_map[csv_table_name]['escaping_options']
            insert_start = time.time()
            self.execute_single_infile(csv_f, csv_table_name, column_insertion_order, extra_const_cols,
                                       escaping_options)
            insert_end = time.time()
            print(f'Inserting {csv_table_name} took {insert_end-insert_start} seconds')

    def validate(self):
        pass

    def run(self):
        run_start = time.time()
        self.detect_fill_date()
        self.validate_extant_csvs()
        self.create_fill_item()
        if self.insert_strategy == 'pandas':
            self.insert_csvs_pandas()
        elif self.insert_strategy == 'infile':
            self.insert_csvs_infile()
        else:
            raise ValueError("No valid insertion strategy provided")
        self.validate()
        run_end = time.time()
        print(f'Running took {run_end-run_start}')

if __name__ == '__main__':
    dump_date = sys.argv[1] if len(sys.argv) >= 2 else None
    dump_subset = sys.argv[2] if len(sys.argv) >= 3 else None
    insert_strategy = sys.argv[3] if len(sys.argv) >= 4 else None
    hdi = humanikiDataInserter(config=os.environ['HUMANIKI_YAML_CONFIG'],
                               dump_date=dump_date,
                               dump_subset=dump_subset,
                               insert_strategy=insert_strategy)
    hdi.run()
