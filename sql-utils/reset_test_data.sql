truncate human_country;
truncate occupation_parent;
truncate human_occupation;
truncate human_property;
truncate human_sitelink;
truncate label;
truncate human;

truncate job;
truncate metric;
truncate metric_aggregations_j;
truncate metric_aggregations_n;
truncate metric_properties_j;
truncate metric_properties_n;
truncate metric_coverage;

delete
from fill
where date = '2020-11-30';