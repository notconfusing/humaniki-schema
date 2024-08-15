import datetime
import gc
import os
import sys
import time
from itertools import combinations, product

import sqlalchemy

from sqlalchemy import func, and_, literal, text, case
from sqlalchemy.orm import aliased
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.expression import not_
from sqlalchemy.sql.functions import count
from sqlalchemy.sql.operators import isnot

from humaniki_schema.queries import get_latest_fill_id, get_properties_obj, NoSuchWikiError, \
    get_exact_fill_id, count_table_metrics, count_table_metric_aggregations_j, count_table_metric_aggregations_n
from humaniki_schema.db import session_factory
from humaniki_schema.schema import human, human_sitelink, human_country, human_occupation, metric, job, metric_coverage, \
    metric_aggregations_j, metric_aggregations_n, project
from humaniki_schema.utils import Properties, PopulationDefinition, get_enum_from_str, read_config_file, \
    make_dump_date_from_str, JobType, JobState
from humaniki_schema.log import get_logger

log = get_logger()


class MetricFactory():
    """
    This class is responsible for
    # load config file
    # generate permutations
    # creating metricCreator objects
      - executing them
    - creating the aggregation_id and property_id tables first if necessary, or cacheing them in memory for fast access
    """

    def __init__(self, config, db_session=None, fill_date=None):
        self.config = read_config_file(config, __file__)
        self.config_generation = self.config['generation']
        self.db_session = db_session if db_session else session_factory()
        if fill_date is None:
            self.curr_fill, self.curr_fill_date = get_latest_fill_id(self.db_session)
        else:
            fill_dt = make_dump_date_from_str(fill_date)
            self.curr_fill, self.curr_fill_date = get_exact_fill_id(self.db_session, fill_dt)
        self.metric_combinations = None
        self.metric_creator = None
        self.metric_job = None
        self.pid = os.getpid()


    def _get_threshold(self, dimension_combination_len):
        try:
            return self.config_generation['combination']['threshold'][dimension_combination_len]
        except KeyError:
            # excepting both either threshold are not present in config at all or not for this length
            return None

    def _generate_metric_combinations(self):
        try:
            combination_config = self.config_generation['combination']
            max_comb_len = combination_config[
                'max_combination_len'] if 'max_combination_len' in combination_config else None

            bias_prop = get_enum_from_str(Properties, combination_config['bias'])
            all_dimensions = [get_enum_from_str(Properties, dim_str) for dim_str in combination_config['dimensions']]
            all_pop_defns = [get_enum_from_str(PopulationDefinition, pop_str) for pop_str in
                             combination_config['population_definitions']]
        except KeyError:
            bias_prop = Properties.GENDER
            all_dimensions = [p for p in Properties.__members__.values() if p != bias_prop]
            all_pop_defns = PopulationDefinition.__members__.values()

        # first create the dimensional combinations
        dimension_combinations = []
        for comb_len in range(len(all_dimensions) + 1):  # +1 because zero indexed
            # check if max combination length is set and if we are passed it
            if max_comb_len is not None and comb_len > max_comb_len:
                log.info(f'PID:{self.pid} Not configured to generate combinations greater than {max_comb_len}')
                continue
            else:
                r_len_combs = list(combinations(all_dimensions, r=comb_len))
                log.info(f'PID:{self.pid} {len(r_len_combs)} of length {comb_len}')
                dimension_combinations.extend(r_len_combs)

        # second product the dimension combinations with the population definitions
        dim_pop_combs_res = product(dimension_combinations, all_pop_defns)
        dim_pop_combs = [{"dimensions": dim_tuple,
                          "population_definition": pop_defn,
                          'threshold': self._get_threshold(len(dim_tuple)),
                          "bias": bias_prop} for (dim_tuple, pop_defn) in dim_pop_combs_res]

        num_dim_combs = len(dimension_combinations)
        num_pop_defns = len(all_pop_defns)
        log.info(f'PID:{self.pid} {len(dim_pop_combs)}==?{num_dim_combs} * {num_pop_defns}')
        self.metric_combinations = dim_pop_combs

    def _get_metric_comb_as_job(self, metric_combination):
        job_query = self.db_session.query(job).filter(
            and_(job.job_type == JobType.METRIC_CREATE.value,
                 job.detail["population_definition"] == metric_combination["population_definition"].value,
                 job.detail["bias_property"] == metric_combination["bias"].value,
                 job.detail["dimension_properties_len"] == len(metric_combination["dimensions"]),
                 job.fill_id == self.curr_fill,
                 ))

        for i, dimension_property in enumerate(metric_combination["dimensions"]):
            job_query = job_query.filter(
                job.detail["dimension_properties"][i] == metric_combination["dimensions"][i].value)

        job_or_none = job_query.one_or_none()
        return job_or_none

    def _persist_metric_combination_as_job(self, metric_combination):
        properties_obj = get_properties_obj(bias_property=metric_combination['bias'].value,
                           dimension_properties=[d.value for d in metric_combination["dimensions"]],
                           session=self.db_session,
                           create_if_no_exist=True)

        mc_job = job(job_type=JobType.METRIC_CREATE.value,
                     job_state=JobState.UNATTEMPTED.value,
                     fill_id=self.curr_fill,
                     detail={"population_definition": metric_combination["population_definition"].value,
                             "bias_property": metric_combination["bias"].value,
                             "dimension_properties": [d.value for d in metric_combination["dimensions"]],
                             "dimension_properties_len": len(metric_combination["dimensions"]),
                             "threshold": metric_combination['threshold'],
                             "properties_id": properties_obj.id,
                             })
        self.db_session.add(mc_job)
        self.db_session.commit()

    def _persist_metric_combinations_as_jobs(self):
        """
        idempotently, store self.metric_combinations in the jobs table
        """
        # for each metrics_combination, attempt to get it's job row, if no exist--create.
        for metric_combination in self.metric_combinations:
            job_or_none = self._get_metric_comb_as_job(metric_combination)
            if job_or_none is not None:
                log.info(f'PID:{self.pid} Job already exists: {metric_combination}')
            else:
                self._persist_metric_combination_as_job(metric_combination)
                log.info(f'PID:{self.pid} Job added: {metric_combination}')

    def _get_uncompleted_metric_create_jobs(self):
        # check the number of inprogress as a safe guard
        in_progress_job_count = self.db_session.query(job).filter(and_(job.job_type == JobType.METRIC_CREATE.value,
                                                                       job.job_state == JobState.IN_PROGRESS.value
                                                                       )).count()
        log.info(f'PID:{self.pid} In progress jobs: {in_progress_job_count}')

        uncompleted_job_states = (JobState.UNATTEMPTED.value,
                                  JobState.NEEDS_RETRY.value)
        uncompleted_jobs_q = self.db_session.query(job).filter(and_(
            job.job_type == JobType.METRIC_CREATE.value,
            job.job_state.in_(uncompleted_job_states)
        ))

        uncompleted_jobs_count = uncompleted_jobs_q.count()
        uncompleted_jobs_first = uncompleted_jobs_q.first()
        log.info(f'PID:{self.pid} There are {uncompleted_jobs_count} uncompleted jobs')
        self.metric_job = uncompleted_jobs_first

    def _create_metric_creators(self):
        if self.metric_job is not None:
            mc = MetricCreator(
                population_definition=PopulationDefinition(self.metric_job.detail["population_definition"]),
                bias_property=Properties(self.metric_job.detail['bias_property']),
                dimension_properties=[Properties(d) for d in self.metric_job.detail['dimension_properties']],
                threshold=self.metric_job.detail['threshold'],
                fill_id=self.metric_job.fill_id,
                properties_id=self.metric_job.detail['properties_id'],
                db_session=session_factory()
            )
            self.metric_creator = mc
            log.info(f"hydrate metric creator")
        else:
            log.info(f'PID:{self.pid} No metrics creator to hydrate')
            sys.exit(29)  # special signal to calling bash.

    def _run_metric_creators(self):
        # strategy_defined = 'execution_strategy' in self.config_generation
        # strategy_sequential = strategy_defined and self.config_generation['execution_stragey'] == 'sequential'
        # if (not strategy_defined) or strategy_sequential:
        # do the sequential
        previous_errors = self.metric_job.errors
        previous_errors = [] if previous_errors is None else previous_errors
        self.metric_job.job_state = JobState.IN_PROGRESS.value
        self.db_session.add(self.metric_job)
        self.db_session.commit()

        try:
            # complete action
            log.info(f"Running: {self.metric_creator}")
            self.metric_creator.run()
            self.metric_job.job_state = JobState.COMPLETE.value
        except Exception as e:
            log.info(f'PID:{self.pid} Encountered {e}')
            next_error = {str(datetime.datetime.utcnow()): repr(e)}
            total_errors = previous_errors + [next_error]
            self.db_session.rollback()
            self.metric_job.errors = total_errors
            flag_modified(self.metric_job, 'errors')
            if len(total_errors) > 3:  # TODO make this configurable
                self.metric_job.job_state = JobState.FAILED.value
            else:
                self.metric_job.job_state = JobState.NEEDS_RETRY.value
        finally:
            self.db_session.add(self.metric_job)
            self.db_session.commit()
            success = self.metric_job.job_state == JobState.COMPLETE.value
            correct_error_count = len(previous_errors) + 1 == len(self.metric_job.errors) if \
                self.metric_job.errors is not None else True
            assert success or correct_error_count

    def create(self):
        metric_run_start = time.time()
        self._generate_metric_combinations()
        self._persist_metric_combinations_as_jobs()
        metric_run_end = time.time()
        log.info(f'PID:{self.pid} Metric Factory creation took {metric_run_end - metric_run_start} seconds')

    def execute(self):
        metric_run_start = time.time()
        self._get_uncompleted_metric_create_jobs()
        self._create_metric_creators()
        self._run_metric_creators()
        metric_run_end = time.time()
        log.info(f'PID:{self.pid} Metric Factory execute took {metric_run_end - metric_run_start} seconds')


class MetricCreator():
    """
    Create a single "metric" (expands to multiple 'metric' rows) based on a population_defintion, bias property, dimension, property, and threshold
    """

    def __init__(self, population_definition, bias_property, dimension_properties, threshold, fill_id, properties_id, db_session):
        self.population_definition = population_definition
        self.population_filter = self._get_population_filter()
        self.coverage_q = None
        self.bias_property = bias_property
        self.dimension_properties = dimension_properties
        self.dimension_properties_pids = [p.value for p in self.dimension_properties]
        self.bias_dimension_properties_pids = [bias_property.value] + self.dimension_properties_pids
        self.dimension_cols = self._get_dim_cols_from_dim_props()
        self.aggregation_ids = None
        self.threshold = threshold
        self.fill_id = fill_id
        self.db_session = db_session
        self.metric_q = None
        self.metric_res = None
        self.insert_metrics = []
        self.metric_properties_id = properties_id

    def __str__(self):
        return f"MetricCreator. bias:{self.bias_property.name}; dimensions:{','.join([d.name for d in self.dimension_properties])}; population:{self.population_definition.name} "


    def _get_population_filter(self):
        pop_filter = {PopulationDefinition.ALL_WIKIDATA: None,
                      PopulationDefinition.GTE_ONE_SITELINK: human.sitelink_count > 0,
                      }
        return pop_filter[self.population_definition]

    def _get_dim_cols_from_dim_props(self):
        """
        note this is for a list of props
        """
        self.col_map = {
            Properties.GENDER: human.gender.label('gender'),
            Properties.PROJECT: human_sitelink.sitelink.label('sitelink'),
            Properties.CITIZENSHIP: human_country.country.label('country'),
            Properties.DATE_OF_BIRTH: human.year_of_birth.label('year_of_birth'),
            Properties.DATE_OF_DEATH: human.year_of_death.label('year_of_death'),
            Properties.OCCUPATION: human_occupation.occupation.label('occupation')}
        return [self.col_map[p] for p in self.dimension_properties]

    def _get_dim_join_from_dim_prop(self, prop):
        """
        note this is for a single prop, not a list of props
        """
        join_table_map = {Properties.PROJECT: human_sitelink,
                          Properties.CITIZENSHIP: human_country,
                          Properties.OCCUPATION: human_occupation,
                          Properties.DATE_OF_BIRTH: None,
                          Properties.DATE_OF_DEATH: None, }
        join_table = join_table_map[prop]
        if join_table is None:  # not a join
            return None, None
        else:
            return join_table, and_(human.qid == join_table.human_id,
                                    human.fill_id == join_table.fill_id)

    def _get_dim_filter_from_dim_prop(self, dim_prop):
        filter_map = {Properties.DATE_OF_BIRTH: human.year_of_birth.isnot(None),
                      Properties.DATE_OF_DEATH: human.year_of_death.isnot(None),
                      Properties.PROJECT: None,
                      Properties.CITIZENSHIP: None,
                      Properties.OCCUPATION: None, }
        return filter_map[dim_prop]

    def _count_metrics_before_and_after(fun):
        def with_counts(self):
            start_metrics_count = count_table_metrics(self.db_session)
            start_metric_aggregations_j_count = count_table_metric_aggregations_j(self.db_session)
            start_metric_aggregations_n_count = count_table_metric_aggregations_n(self.db_session)
            fun(self)
            end_metrics_count = count_table_metrics(self.db_session)
            end_metric_aggregations_j_count = count_table_metric_aggregations_j(self.db_session)
            end_metric_aggregations_n_count = count_table_metric_aggregations_n(self.db_session)
            metric_additions = end_metrics_count - start_metrics_count
            metric_aggregations_j_additions = end_metric_aggregations_j_count - start_metric_aggregations_j_count
            metric_aggregations_n_additions = end_metric_aggregations_n_count - start_metric_aggregations_n_count
            metric_n_j_ratio = metric_aggregations_n_additions / metric_aggregations_j_additions if metric_aggregations_j_additions != 0 else None
            log.info(f'Metric Counter: {metric_additions} metrics added')
            log.info(f'Metric Counter: {metric_aggregations_j_additions} metric_aggregations_j added')
            log.info(f'Metric Counter: {metric_aggregations_n_additions} metric_aggregations_n added')
            log.info(f'Metric Counter: {metric_n_j_ratio} n to j ratio')
            if metric_n_j_ratio:
                assert metric_n_j_ratio == len(
                    self.bias_dimension_properties_pids), 'not adding aggregations in correct ratio. ratio was' \
                                                          f'{metric_n_j_ratio} and we expected' \
                                                          f' {len(self.bias_dimension_properties_pids)}'

        return with_counts

    def _time_step(fun):
        def with_timing(self):
            start = time.time()
            fun(self)
            end = time.time()
            log.info(f'Stage timing {fun.__name__} took: {round(end - start)} seconds')

        return with_timing

    def make_agg_humans_query(self):
        """        make something approximating
        metric_q = db_session.query(human.gender, human_sitelink.sitelink, human_country.country,
                                    func.count(human.gender)) \
            .join(human_country, and_(human.qid == human_country.human_id, human.fill_id == human_country.fill_id)) \
            .join(human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)) \
            .group_by(human_country.country, human_sitelink.sitelink, human.gender)"""
        bias_col = human.gender.label('gender')  # maybe getattr(human, bias_property.name.lower()
        count_col = func.count(bias_col).label('total')
        metric_cols = [bias_col, *self.dimension_cols, count_col]
        group_bys = [bias_col] + self.dimension_cols

        metric_q = self.db_session.query(*metric_cols)
        # dimension joins and filters
        for dim_prop in self.dimension_properties:
            ## apply joins
            join_table, join_on = self._get_dim_join_from_dim_prop(dim_prop)
            if join_table is not None:
                metric_q = metric_q.join(join_table, join_on)
            ## apply filters
            filter = self._get_dim_filter_from_dim_prop(dim_prop)
            if filter is not None:
                metric_q = metric_q.filter(filter)

        if self.population_filter is not None:
            metric_q = metric_q.filter(self.population_filter)

        # add gender is not null
        metric_q = metric_q.filter(isnot(human.gender, None))

        # current fill filter
        metric_q = metric_q.filter(human.fill_id == self.fill_id)
        metric_q = metric_q.group_by(*group_bys)
        if self.threshold:
            metric_q = metric_q.having(count_col >= self.threshold)
        metric_q_sub = metric_q.subquery('grouped')
        metric_raw_sql = metric_q.statement.compile(compile_kwargs={"literal_binds": True})
        # log.debug(f'compiled metric sql is: {metric_raw_sql}')
        self.metric_q = metric_q
        return metric_q_sub

    def make_human_2_maj_insert_query(self, metric_q_sub):
        maj_joins = []
        dim_cols_of_metric_q_sub = []
        for i, dim_col in enumerate(self.dimension_cols):
            # grouped.sitelink = JSON_UNQUOTE(JSON_EXTRACT(existing_aggs.aggregations, '$[0]'))
            dim_col_of_metric_q_sub = getattr(metric_q_sub.c, dim_col.key)
            maj_join = dim_col_of_metric_q_sub == \
                       func.JSON_UNQUOTE(func.JSON_EXTRACT(metric_aggregations_j.aggregations, f"$[{i}]"))
            maj_joins.append(maj_join)
            dim_cols_of_metric_q_sub.append(dim_col_of_metric_q_sub)

        deduped_q = self.db_session.query(
            metric_q_sub.c.gender.label('bias_value'),
            func.JSON_ARRAY(*dim_cols_of_metric_q_sub).label('aggregations'),
            literal(len(self.dimension_cols)).label('aggregations_len'),
            func.JSON_ARRAY(*self.dimension_properties_pids).label('properties'),
        ).select_from(metric_q_sub) \
            .join(metric_aggregations_j,
                  and_(metric_q_sub.c.gender == metric_aggregations_j.bias_value,
                       metric_aggregations_j.aggregations_len == len(self.dimension_properties),
                       *maj_joins),
                  isouter=True  # left join
                  ) \
            .filter(metric_aggregations_j.id.is_(None))  # not already present maj

        # dedupe_raw_sql = deduped_q.statement.compile(compile_kwargs={"literal_binds": True})
        # log.debug(f'compiled dedupe sql is: {dedupe_raw_sql}')

        maj_insert = sqlalchemy \
            .insert(metric_aggregations_j) \
            .prefix_with('IGNORE') \
            .from_select(names=['bias_value', 'aggregations', 'aggregations_len', 'properties'],
                         select=deduped_q)
        maj_sql = maj_insert.compile(compile_kwargs={'literal_binds': True})
        log.debug(maj_sql)
        return maj_insert

    @_time_step
    def step_one_maj_insert(self):
        # create sub queries and wrapping queries
        metric_q_sub = self.make_agg_humans_query()
        maj_insert = self.make_human_2_maj_insert_query(metric_q_sub=metric_q_sub)

        self.db_session.execute(maj_insert)
        self.db_session.commit()

    @_time_step
    def step_two_man_insert(self):
        '''Doing this as raw sql because the JSON_TABLE function is particularly hard in sqlalchemy'''
        when_temp = 'when aggregation_order - 1 = {i} then {prop_pid}'
        whens = [when_temp.format(i=i, prop_pid=prop_pid) for i, prop_pid in
                 enumerate(self.bias_dimension_properties_pids)]
        whens_str = '\n'.join(whens)
        properties_case_statement = f"""CASE {whens_str} END  as property"""

        properties_eq_temp = """ metric_aggregations_j.properties->'$[{i}]' = {prop_pid}"""
        # i reall dislike that in normalized version bias is included as tbe 0th agg, and in the json version not, but that's how it is
        properties_eqs = [properties_eq_temp.format(i=i, prop_pid=prop_pid) for i, prop_pid in
                          enumerate(self.dimension_properties_pids)]
        properties_correct = ' AND '.join(properties_eqs)
        another_and = 'AND' if len(self.dimension_properties) > 0 else ''
        properties_correct = another_and + properties_correct

        maj_to_man = f"""
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
                            -- even though this will get converted into an int, needs to support alpha wikicodes first.
                           value varchar(255) path '$[0]'
                           )
                   ) as v
          where metric_aggregations_j.aggregations_len = {len(self.dimension_properties)}
               {properties_correct}
         ),
        intified as (
         select e.id,
                {properties_case_statement},
                -- select either the wikicode id if it was joinable, or the nowikicode value
                COALESCE(p.id, e.value) as value, -- if the wiki does not exist, null will result, and that will get stored as a 0.
                aggregation_order - 1   as aggregation_order
         from exploded e
                  left join project p
                            on e.value = p.code
             )
        SELECT id, property, value, aggregation_order
        FROM intified;
        """
        log.debug(f' man to maj statement: {maj_to_man}')
        self.db_session.execute(maj_to_man)
        self.db_session.commit()

    def _make_agg_n_wide_project_case_statement(self, man):
        # when the property is project/sitelink return the project code
        return case([(man.property == 0,
                      project.code), ],
                    else_=man.value)

    def make_metric_agg_n_wide(self):
        # its actually faster than joining on j
        try:
            project_pos = self.bias_dimension_properties_pids.index(Properties.PROJECT.value)
        except ValueError:
            project_pos = None

        mans = [aliased(metric_aggregations_n, name='n' * (i + 1)) for i in
                range(len(self.bias_dimension_properties_pids))]
        man_extra = aliased(metric_aggregations_n,
                            name='extra')  # this extra table allows us to filter out aggregation ids that match because they include the same elements at the smaller but have more properties and values
        assert len(mans) >= 1, 'should be at least lenght 1'
        # query_cols = [self._make_agg_n_wide_project_case_statement(man_i).label(f'val_{i}') for i, man_i in enumerate(mans)]  # add in case statement if known to be sitelink
        query_cols = [
            project.code.label(f'val_{i}') if project_pos and project_pos == i else man_i.value.label(f'val_{i}') for
            i, man_i in enumerate(mans)]  # add in case statement if known to be sitelink
        man_wide_q = self.db_session.query(mans[0].id,
                                           *query_cols) \
            .select_from(mans[0])

        for i, man_i in enumerate(mans):
            if i == 0:
                continue
            else:
                man_wide_q = man_wide_q.join(man_i,
                                             and_(mans[i - 1].id == man_i.id,
                                                  mans[i - 1].aggregation_order == i - 1,
                                                  mans[i - 1].property == self.bias_dimension_properties_pids[i - 1],
                                                  man_i.aggregation_order == i,
                                                  man_i.property == self.bias_dimension_properties_pids[i])
                                             )

        man_wide_q = man_wide_q.join(man_extra,
                                     and_(mans[0].id == man_extra.id,
                                          mans[0].aggregation_order == 0,
                                          man_extra.aggregation_order == len(self.bias_dimension_properties_pids),
                                          mans[0].property == self.bias_dimension_properties_pids[0],
                                          not_(man_extra.property.in_(self.bias_dimension_properties_pids))
                                          ),
                                     isouter=True
                                     )
        man_wide_q = man_wide_q.filter(man_extra.id.is_(None))

        if project_pos:
            man_wide_q = man_wide_q.join(project, mans[project_pos].value == project.id, isouter=True)

        for i, man_i in enumerate(mans):
            man_wide_q = man_wide_q.filter(man_i.aggregation_order == i)

        man_wide_sql = man_wide_q.statement.compile(compile_kwargs={"literal_binds": True})
        # log.debug(f'man wide sql is: {man_wide_sql}')
        return man_wide_q.subquery('man_wide')

    @_time_step
    def step_three_human_with_man_insert(self):
        metric_q_sub = self.make_agg_humans_query()
        agg_n_wide = self.make_metric_agg_n_wide()

        on_clauses = []
        for i, pid in enumerate(self.bias_dimension_properties_pids):
            metric_targ_col = self.col_map[Properties(pid)]
            metric_col = getattr(metric_q_sub.c, metric_targ_col.name)
            agg_targ_col = f'val_{i}'
            agg_n_col = getattr(agg_n_wide.c, agg_targ_col)
            on_clauses.append(metric_col == agg_n_col)

        metric_w_agg = self.db_session.query(
            literal(self.fill_id).label('fill_id'),
            literal(self.population_definition.value).label('population_id'),
            literal(self.metric_properties_id).label('properties_id'),
            agg_n_wide.c.id.label('aggrgations_id'),
            metric_q_sub.c.gender.label('bias_value'),
            metric_q_sub.c.total.label('total')
        ).select_from(
            metric_q_sub
        ).join(
            agg_n_wide,
            and_(*on_clauses)
        )

        metric_w_agg_insert = sqlalchemy \
            .insert(metric) \
            .prefix_with('IGNORE') \
            .from_select(names=['fill_id', 'population_id', 'properties_id', 'aggregations_id', 'bias_value', 'total'],
                         select=metric_w_agg)

        metric_w_agg_insert_sql = metric_w_agg_insert.compile(compile_kwargs={'literal_binds': True})
        log.debug(f'man with agg sql is: {metric_w_agg_insert_sql}')

        self.db_session.execute(metric_w_agg_insert)
        self.db_session.commit()

    @_count_metrics_before_and_after
    def compile(self):
        self.step_one_maj_insert()
        self.step_two_man_insert()
        self.step_three_human_with_man_insert()


    @_time_step
    def generate_coverage(self):
        """store the total number of items with these properties
                and the total numeber of sitelinks"""
        # going for something like
        # select count(1) as total_items , sum(sitelink_count) total_sitelinks_with_prop
        # from
        # (SELECT qid, min(sitelink_count) as sitelink_count -- min should work because all the sitelink counts should be the same
        # FROM human JOIN human_sitelink ON human.qid = human_sitelink.human_id
        #       AND human.fill_id = human_sitelink.fill_id JOIN human_country ON human.qid = human_country.human_id
        #        AND human.fill_id = human_country.fill_id
        # WHERE human.gender IS NOT NULL AND human.fill_id = 68
        # group by qid) items_with_prop

        # first make a subquery of items with props
        bias_col = human.gender  # maybe getattr(human, bias_property.name.lower()
        group_bys = [human.qid]

        item_prop_q = self.db_session.query(human.qid.label('qid'),
                                            func.min(human.sitelink_count).label('sitelink_count'))
        # dimension joins and filters
        for dim_prop in self.dimension_properties:
            ## apply joins
            join_table, join_on = self._get_dim_join_from_dim_prop(dim_prop)
            if join_table is not None:
                item_prop_q = item_prop_q.join(join_table, join_on)
            ## apply filters
            filter = self._get_dim_filter_from_dim_prop(dim_prop)
            if filter is not None:
                item_prop_q = item_prop_q.filter(filter)

        if self.population_filter is not None:
            item_prop_q = item_prop_q.filter(self.population_filter)

        # add gender is not null
        item_prop_q = item_prop_q.filter(isnot(human.gender, None))

        # current fill filter
        item_prop_q = item_prop_q.filter(human.fill_id == self.fill_id)
        item_prop_q = item_prop_q.group_by(*group_bys)
        items_with = item_prop_q.subquery().alias('items_with')

        # second, count the number of items and sitelinks
        coverage_q = sqlalchemy.select(
            [literal(f'{self.fill_id}').label('fill_id'),
             literal(f'{self.metric_properties_id}').label('properties_id'),
             literal(f'{self.population_definition.value}').label('population_id'),
             func.count(items_with.c.qid).label('total_with_properties'),
             func.sum(items_with.c.sitelink_count).label('total_sitelinks_with_properties')])

        # insert into metric_coverage
        coverage_insert = sqlalchemy \
            .insert(metric_coverage) \
            .prefix_with('IGNORE') \
            .from_select(names=['fill_id', 'properties_id',
                                'population_id',
                                'total_with_properties', 'total_sitelinks_with_properties'],
                         # array of column names that your query returns
                         select=coverage_q)  # your query or other select() object
        coverage_sql = coverage_insert.compile(compile_kwargs={'literal_binds': True})
        log.debug(coverage_sql)
        coverage_res = self.db_session.execute(coverage_insert)
        self.db_session.commit()

        return coverage_res

    def run(self):
        # can do timing here
        self.generate_coverage()
        self.compile()



if __name__ == '__main__':
    create_execute = sys.argv[1] if len(sys.argv) >= 2 else None
    dump_date = sys.argv[2] if len(sys.argv) >= 3 else None
    this_pid = os.getpid()
    log.info(f'PID:{this_pid} Specified dump date: {dump_date}. Create or execute is: {create_execute}')
    mf = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'], fill_date=dump_date)
    if create_execute:
        log.info(f"Attempting to run {create_execute} on metrics factory")
        getattr(mf, create_execute)()

    log.info("Generate metrics---DONE")
