from enum import Enum

WMF_TIMESTAMP_FMT = '%a %b %d %H:%M:%S %z %Y'


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
