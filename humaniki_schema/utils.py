import os
from enum import Enum
# TODO, consider Immutable dict instead
from datetime import datetime
from pathlib import Path
import yaml

WMF_TIMESTAMP_FMT = '%a %b %d %H:%M:%S %z %Y'
HUMANIKI_SNAPSHOT_DATE_FMT = '%Y-%m-%d'

def make_fill_dt(snapshot_str):
    return datetime.strptime(snapshot_str, HUMANIKI_SNAPSHOT_DATE_FMT)


class Properties(Enum):
    PROJECT = 0 # faking this as P0
    CITIZENSHIP = 27
    DATE_OF_BIRTH = 569
    DATE_OF_DEATH = 570
    OCCUPATION = 106
    GENDER = 21


class FillType(Enum):
    DUMP = 1
    RECENT_CHANGES = 2


class MetricFacets(Enum):
    GEOGRAPHY = 1
    EVENT_YEAR = 2

class PopulationDefinition(Enum):
    ALL_WIKIDATA = 1
    GTE_ONE_SITELINK = 2
    SITELINK_MULTIPLICITY = 3



def read_config_file(config_file_name, caller__file__):
    # TODO: this config reader requires you calling it from the a place that is not symlinked
    # at the .parents mechanism sees not be able to jump those backwards
    # for instance if you have project/lib/civilservant-core/civilservant/make_experiment.py
    # you cant do cd lib/civilservant-core/civilservant && python make_experiment.py but rather need to do
    # the easier thing of cd project && python lib/civilservant-core/civilservant/make_experiment.py
    for i in range(4):
        try:
            ancestor_dir = Path(caller__file__).parents[i]
            config_loc = os.path.join(ancestor_dir, 'config', config_file_name)
            config = yaml.safe_load(open(config_loc, 'r'))
            if config:
               return config
        except FileNotFoundError:
                pass
    # we got to the end without finding a config
    raise FileNotFoundError(config_file_name)
