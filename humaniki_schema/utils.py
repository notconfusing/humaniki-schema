import os
from enum import Enum
# TODO, consider Immutable dict instead
from datetime import datetime, date
from pathlib import Path
import yaml


WMF_TIMESTAMP_FMT = '%a %b %d %H:%M:%S %z %Y'
HUMANIKI_SNAPSHOT_DATE_FMT = '%Y%m%d'
HUMANIKI_SNAPSHOT_DATETIME_FMT = '%Y%m%d %H:%M:%S'


def make_fill_dt(snapshot_str):
    return datetime.strptime(snapshot_str, HUMANIKI_SNAPSHOT_DATE_FMT)


class Properties(Enum):
    PROJECT = 0  # faking this as P0
    CITIZENSHIP = 27
    DATE_OF_BIRTH = 569
    DATE_OF_DEATH = 570
    OCCUPATION = 106
    GENDER = 21


def order_props(props):
    return sorted(props, key=lambda p: p.value)


class FillType(Enum):
    DUMP = 1
    RECENT_CHANGES = 2


class MetricFacets(Enum):
    GEOGRAPHY = 1
    EVENT_YEAR = 2


class PopulationDefinition(Enum):
    ALL_WIKIDATA = 1
    GTE_ONE_SITELINK = 2
    # SITELINK_MULTIPLICITY = 3


class JobType(Enum):
    DUMP_PARSE = 1
    INSERT = 2
    METRIC_CREATE = 3


class JobState(Enum):
    UNATTEMPTED = 1
    IN_PROGRESS = 2
    COMPLETE = 3
    NEEDS_RETRY = 4
    FAILED = 5


def get_enum_from_str(enum_class, s):
    try:
        return getattr(enum_class, s.upper())
    except AttributeError:
        return None


def read_config_file(config_file_name, caller__file__):
    # TODO: this config reader requires you calling it from the a place that is not symlinked
    # as the .parents mechanism sees not be able to jump those backwards
    # for instance if you have project/lib/civilservant-core/civilservant/make_experiment.py
    # you cant do cd lib/civilservant-core/civilservant && python make_experiment.py but rather need to do
    # the easier thing of cd project && python lib/civilservant-core/civilservant/make_experiment.py
    above_conf_dir = get_ancestor_directory_that_has_xdir_as_child('config', caller__file__)
    config_loc = os.path.join(above_conf_dir, 'config', config_file_name)
    config = yaml.safe_load(open(config_loc, 'r'))
    return config

def get_ancestor_directory_that_has_xdir_as_child(xdir, caller__file__):
    '''Go up from the caller__file__ until xdir is a child of curr dir '''
    start_dir = os.path.abspath(caller__file__)
    for i in range(4):
        ancestor_dir = Path(start_dir).parents[i]
        if xdir in os.listdir(ancestor_dir):
            return ancestor_dir
        else:
            continue
    raise FileNotFoundError

def make_dump_date_from_str(datestr):
    if isinstance(datestr, date):
        # actually you passed a date already
        return datestr
    else:
        return datetime.strptime(datestr, HUMANIKI_SNAPSHOT_DATE_FMT).date()


def is_wikimedia_cloud_dump_format(path_filename):
    filename = os.path.basename(path_filename)
    filename_parts = filename.split('.')
    correct_part_nums = len(filename_parts) ==3
    if correct_part_nums:
        # part_one_is_date = len(filename_parts[0])==8 and filename_parts[0].isnumeric()
        numeric_part = numeric_part_of_filename(filename)
        part_one_is_date = len(numeric_part)==8 and numeric_part.isnumeric()
        part_two_is_json = filename_parts[1] == 'json'
        part_three_is_gz = filename_parts[2] == 'gz'
        return part_one_is_date and part_two_is_json and part_three_is_gz
    else:
        return False

def numeric_part_of_filename(filename, fformat='dashed', basenameittoo=False):
    filename = os.path.basename(filename) if basenameittoo else filename
    if fformat=='dashed':
        # something like wikidata-YYYYMMDD-all.json.gz
        without_extensions = filename.split('.')[0]
        dash_parts = without_extensions.split('-')
        return dash_parts[1]