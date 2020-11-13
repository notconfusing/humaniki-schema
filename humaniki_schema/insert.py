import datetime
import json
import os
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
    def __init__(self, config, dump_date=None):
        self.config = hs_utils.read_config_file(config, __file__)
        self.config_insertion = self.config['insertion']
        self.overwrite = self.config_insertion['overwrite'] if 'overwrite' in self.config_insertion else False
        self.dump_date = dump_date
        self.dump_date_str = None
        self.fill_id = None
        self.detection_type = None
        self.db_session = session_factory()
        # order is important becuse of foreign key constraint
        self.csvs = None
        self.CSV_NA_VALUE = r'\N'

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
        csv_dir = os.path.join(self.config_insertion['wdtk_processing_output'], self.dump_date.strftime('%Y%m%d'))
        all_files = os.listdir(csv_dir)
        expected_csvs = ["occupation_parent.csv", "label.csv", "human_country.csv", "human_occupation.csv", "human.csv",
                         "human_sitelink.csv", ]
        extant_csvs = [f for f in all_files if f.endswith('.csv')]
        self.csvs = []
        for csv in extant_csvs:
            if csv not in expected_csvs:
                print(f'Not going to process: {csv}')
            else:
                self.csvs.append(csv)
        assert len(self.csvs) == len(expected_csvs)

    def create_fill_item(self):
        prev_latest_fill_id, prev_latest_fill_dt = get_latest_fill_id(self.db_session)
        if prev_latest_fill_dt == self.dump_date:
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
        self._persist_rows(insert_rows)

    def insert_csvs(self):
        table_column_map = {
            'human': ['qid', 'gender', 'year_of_birth', 'sitelink_count'],
            'human_country': ['human_id', 'country'],
            'human_occupation': ['human_id', 'occupation'],
            'human_sitelink': ['human_id', 'sitelink'],
            'label': ['qid', 'label'],
            'occupation_parent': ['occupation', 'parent'],
        }
        for csv in self.csvs:
            csv_f = os.path.join(self.config_insertion['wdtk_processing_output'], self.dump_date_str, csv)
            csv_table_name = csv.split('.csv')[0]
            schema_table = getattr(humaniki_schema.schema, csv_table_name)
            self._insert_csv(table_f=csv_f,
                             extra_const_cols=None,
                             schema_table=schema_table,
                             columns=table_column_map[csv_table_name])

    def validate(self):
        pass

    def run(self):
        self.detect_fill_date()
        self.validate_extant_csvs()
        self.create_fill_item()
        self.insert_csvs()
        self.validate()


if __name__ == '__main__':
    hdi = humanikiDataInserter(os.environ['HUMANIKI_YAML_CONFIG'])
    hdi.run()
