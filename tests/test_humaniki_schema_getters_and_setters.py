import json
import os
import pandas as pd
import time

import pytest
from sqlalchemy import func

from humaniki_schema import db
from humaniki_schema.queries import get_aggregations_obj, get_latest_fill_id
from humaniki_schema.schema import metric, metric_aggregations_n
from humaniki_schema.utils import read_config_file

config = read_config_file(os.environ['HUMANIKI_YAML_CONFIG'], __file__)

session = db.session_factory()


skip_generation = config['test']['skip_gen'] if 'skip_gen' in config['test'] else False
if not skip_generation:
    raise AssertionError('''generation must happen by orchestration now
    try: `orchestrate.py with env HUMANIKI_MAX_HUMANS=100` to get tests ''')
    print(f'generated: {generated}')
    session = db.session_factory()
    metrics_count = session.query(func.count(metric.fill_id)).scalar()
    print(f'number of metrics: {metrics_count}')
    assert metrics_count>0


@pytest.fixture
def test_jsons():
    test_files = {}
    test_datadir = config['test']['test_datadir']
    files = os.listdir(test_datadir)
    json_fs = [f for f in files if f.endswith('.json')]
    for json_f in json_fs:
        j = json.load(open(os.path.join(test_datadir, json_f)))
        test_files[json_f] = j
    return test_files


## NOTE: assumes the metrics built on the first 10 humans, from exmaple data

def test_get_aggregations_n(test_jsons):
    ordered_aggregations = {569: '1952'}
    aggregation_objs = get_aggregations_obj(bias_value=None, dimension_values=ordered_aggregations,
                                            session=session, table=metric_aggregations_n)
    assert isinstance(aggregation_objs, list)
    assert len(aggregation_objs) == 2  # there should be two aggregations represented, the gender and the date of birth
    dob_rows = [row for row in aggregation_objs if row.property == 569]
    assert len(dob_rows) == 1
    assert dob_rows[0].value == 1952
