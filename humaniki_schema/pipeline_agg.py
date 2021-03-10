# TEST: after a run of orchestrate. on 2020-11-30, with max_humans = 300
# counts for
# 38866,metric
# 37018, metric_agg_j
# 125080,metric_agg_n


def human_to_maj_temp(table_col_tups):
    grouped_agg_cols = ','.join([f'{table}.{col}' for (table, col) in table_col_tups])
    return f'''-- step 1. agg to maj
INSERT IGNORE INTO
    metric_aggregations_j(bias_value, aggregations, aggregations_len)
WITH grouped as (
    SELECT human.gender, {grouped_agg_cols}, count(human.gender) AS count_1
          FROM human
                   JOIN human_sitelink ON human.qid = human_sitelink.human_id AND human.fill_id = human_sitelink.fill_id
          WHERE human.gender IS NOT NULL
            AND human.fill_id = 105
          GROUP BY human.gender, human_sitelink.sitelink
), deduped as (
SELECT
        gender  as bias_value,
       -- TODO: autoamtion row
       JSON_ARRAY(sitelink)              as aggregations,
       JSON_LENGTH(JSON_ARRAY(sitelink)) as aggregations_len
    FROM grouped
        -- this dedupes to avoid
    LEFT JOIN metric_aggregations_j existing_aggs
        on grouped.gender= existing_aggs.bias_value
           -- TODO: automation row
           and grouped.sitelink = JSON_UNQUOTE(JSON_EXTRACT(existing_aggs.aggregations, '$[0]'))
            and aggregations_len = 1
    WHERE existing_aggs.id is NULL
    )
SELECT bias_value, aggregations, aggregations_len
FROM deduped;'''

human_to_maj = f'''-- step 1. agg to maj
-- TODO: redo with CTE.
INSERT IGNORE INTO
    metric_aggregations_j(bias_value, aggregations, aggregations_len)
WITH grouped as (
    SELECT human.gender, human_sitelink.sitelink, count(human.gender) AS count_1
          FROM human
                   JOIN human_sitelink ON human.qid = human_sitelink.human_id AND human.fill_id = human_sitelink.fill_id
          WHERE human.gender IS NOT NULL
            AND human.fill_id = 105
          GROUP BY human.gender, human_sitelink.sitelink
), deduped as (
SELECT
        gender  as bias_value,
       -- TODO: autoamtion row
       JSON_ARRAY(sitelink)              as aggregations,
       JSON_LENGTH(JSON_ARRAY(sitelink)) as aggregations_len
    FROM grouped
        -- this dedupes to avoid
    LEFT JOIN metric_aggregations_j existing_aggs
        on grouped.gender= existing_aggs.bias_value
           -- TODO: automation row
           and grouped.sitelink = JSON_UNQUOTE(JSON_EXTRACT(existing_aggs.aggregations, '$[0]'))
            and aggregations_len = 1
    WHERE existing_aggs.id is NULL
    )
SELECT bias_value, aggregations, aggregations_len
FROM deduped;'''

maj_to_man = '''
INSERT IGNORE INTO metric_aggregations_n(id, property, value, aggregation_order)
with exploded as
         (select id,
                 v.*
          FROM metric_aggregations_j,
               JSON_TABLE(
                       -- since im going wide to long, make this really wide first
                       JSON_ARRAY_INSERT(aggregations, '$[0]', bias_value),
                       "$[*]"
                       COLUMNS (
                           aggregation_order for ordinality,
                           value varchar(255) path '$[0]'
                           )
                   ) as v
          -- TODO: automation
          where metric_aggregations_j.aggregations_len = 2
             ),
intified as (
    select e.id,
          CASE
               when aggregation_order - 1 = 0 then 21
               when aggregation_order - 1 = 1 then 0
               when aggregation_order - 1 = 2 then 27
               END                 as property,
           -- select either the wikicode id if it was joinable, or the nowikicode value
           COALESCE(p.id, e.value) as value, -- will need a nosuchwiki solution still
            aggregation_order - 1   as aggregation_order
    from exploded e
             left join project p
                       on e.value = p.code
)
SELECT id, property, value, aggregation_order
FROM intified;'''

human_to_man_ins = '''
-- step 3: join the agg with the agg-ids and insert the ids into metrics
INSERT IGNORE INTO metric(fill_id, population_id, properties_id, aggregations_id, bias_value, total)
WITH grouped as (
    SELECT human.gender, human_sitelink.sitelink, human_country.country, count(human.gender) AS count_1
    FROM human
             JOIN human_sitelink ON human.qid = human_sitelink.human_id AND human.fill_id = human_sitelink.fill_id
             JOIN human_country ON human.qid = human_country.human_id AND human.fill_id = human_country.fill_id
    WHERE human.gender IS NOT NULL
      AND human.fill_id = 105
    GROUP BY human.gender, human_sitelink.sitelink, human_country.country
),
-- question is whether to use the j or n version of aggregations
-- j would require joining on json values (maybe slow?)
-- n would require joining the sitelinks with project (maybe less slow?)
     naggs as (
         select n.id,
                n.value as val1,
                                         case
                    when nn.property = 0 then p.code
                    else nn.value
                    end as val2,
                nnn.value as val3
         from metric_aggregations_n n
            join metric_aggregations_n nn on n.id = nn.id
            join metric_aggregations_n nnn on nn.id = nnn.id
            left join project p
                            on nn.value = p.id
         where n.aggregation_order=0
         and nn.aggregation_order=1
         and nnn.aggregation_order=2
         -- TODO actually need to join this even one more time and ensure the n+1'th order is null
          -- to preclude at 4 value aggregation coming up in 3-value scenarios
 -- don't know if i can make a better filter than that
     )
select -1             as fill_id,
       -1             as population_id,
       -1             as properties_id,
       naggs.id      as aggergations_id,
       grouped.gender as bias_value,
       grouped.count_1 as total
from grouped
join naggs
 on grouped.gender = naggs.val1
and grouped.sitelink = naggs.val2
and grouped.country = naggs.val3
'''