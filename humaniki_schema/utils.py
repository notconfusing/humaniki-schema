from enum import Enum
# TODO, consider Immutable dict instead
from datetime import datetime

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

