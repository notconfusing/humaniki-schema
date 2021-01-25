import datetime
import gc
import os
import sys
import time
from itertools import combinations, product

import sqlalchemy

from sqlalchemy import func, and_
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.functions import count
from sqlalchemy.sql.operators import isnot

from humaniki_schema.queries import get_latest_fill_id, AggregationIdGetter, get_properties_obj, NoSuchWikiError, \
    get_exact_fill_id
from humaniki_schema.db import session_factory
from humaniki_schema.schema import human, human_sitelink, human_country, human_occupation, metric, job
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
                log.info(f'Not configured to generate combinations greater than {max_comb_len}')
                continue
            else:
                r_len_combs = list(combinations(all_dimensions, r=comb_len))
                log.info(f'{len(r_len_combs)} of length {comb_len}')
                dimension_combinations.extend(r_len_combs)

        # second product the dimension combinations with the population definitions
        dim_pop_combs_res = product(dimension_combinations, all_pop_defns)
        dim_pop_combs = [{"dimensions": dim_tuple,
                          "population_definition": pop_defn,
                          "bias": bias_prop} for (dim_tuple, pop_defn) in dim_pop_combs_res]

        num_dim_combs = len(dimension_combinations)
        num_pop_defns = len(all_pop_defns)
        log.info(f'{len(dim_pop_combs)}==?{num_dim_combs} * {num_pop_defns}')
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

        # jq_str = job_query.statement.compile(compile_kwargs={"literal_binds":True}).string
        job_or_none = job_query.one_or_none()
        return job_or_none

    def _persist_metric_combination_as_job(self, metric_combination):
        mc_job = job(job_type=JobType.METRIC_CREATE.value,
                     job_state=JobState.UNATTEMPTED.value,
                     fill_id=self.curr_fill,
                     detail={"population_definition": metric_combination["population_definition"].value,
                             "bias_property": metric_combination["bias"].value,
                             "dimension_properties": [d.value for d in metric_combination["dimensions"]],
                             "dimension_properties_len": len(metric_combination["dimensions"]),
                             "thresholds": None,
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
                log.info(f'Job already exists: {metric_combination}')
            else:
                self._persist_metric_combination_as_job(metric_combination)
                log.info(f'Job added: {metric_combination}')

    def _get_uncompleted_metric_create_jobs(self):
        # check the number of inprogress as a safe guard
        in_progress_job_count = self.db_session.query(job).filter(and_(job.job_type == JobType.METRIC_CREATE.value,
                                                                       job.job_state == JobState.IN_PROGRESS.value
                                                                       )).count()
        log.info(f'In progress jobs: {in_progress_job_count}')

        uncompleted_job_states = (JobState.UNATTEMPTED.value,
                                  JobState.NEEDS_RETRY.value)
        uncompleted_jobs_q = self.db_session.query(job).filter(and_(
            job.job_type == JobType.METRIC_CREATE.value,
            job.job_state.in_(uncompleted_job_states)
        ))

        uncompleted_jobs_count = uncompleted_jobs_q.count()
        uncompleted_jobs_first = uncompleted_jobs_q.first()
        log.info(f'There are {uncompleted_jobs_count} uncompleted jobs')
        self.metric_job = uncompleted_jobs_first


    def _create_metric_creators(self):
        if self.metric_job is not None:
            mc = MetricCreator(population_definition=PopulationDefinition(self.metric_job.detail["population_definition"]),
                               bias_property=Properties(self.metric_job.detail['bias_property']),
                               dimension_properties=[Properties(d) for d in self.metric_job.detail['dimension_properties']],
                               thresholds=self.metric_job.detail['thresholds'],
                               fill_id=self.metric_job.fill_id,
                               db_session=session_factory()
                               )
            self.metric_creator = mc
            log.info(f"hydrate metric creator")
        else:
            log.info(f'No metrics creator to hydrate')
            sys.exit(29) #special signal to calling bash.

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
            log.info(f'Encountered {e}')
            next_error = {str(datetime.datetime.utcnow()):repr(e)}
            total_errors = previous_errors + [next_error]
            self.db_session.rollback()
            self.metric_job.errors = total_errors
            flag_modified(self.metric_job, 'errors')
            if len(total_errors) > 3: # TODO make this configurable
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
        log.info(f'Metric Factory creation took {metric_run_end - metric_run_start} seconds')

    def execute(self):
        metric_run_start = time.time()
        self._get_uncompleted_metric_create_jobs()
        self._create_metric_creators()
        self._run_metric_creators()
        metric_run_end = time.time()
        log.info(f'Metric Factory execute took {metric_run_end - metric_run_start} seconds')


class MetricCreator():
    """
    Create a single "metric" (expands to multiple 'metric' rows) based on a population_defintion, bias property, dimension, property, and thresholds
    """

    def __init__(self, population_definition, bias_property, dimension_properties, thresholds, fill_id, db_session):
        self.population_definition = population_definition
        self.coverage_q = None
        self.bias_property = bias_property
        self.dimension_properties = dimension_properties
        self.dimension_properties_pids = [p.value for p in self.dimension_properties]
        self.aggregation_ids = None
        self.properties_ids = None
        self.thresholds = thresholds
        self.fill_id = fill_id
        self.db_session = db_session
        self.metric_q = None
        self.metric_res = None
        self.insert_metrics = []
        self.aggregation_getter = AggregationIdGetter(bias=self.bias_property, props=self.dimension_properties)
        self.metric_properties_id = self._get_metric_properties_id()

    def __str__(self):
        return f"MetricCreator. bias:{self.bias_property.name}; dimensions:{','.join([d.name for d in self.dimension_properties])}; population:{self.population_definition.name} "

    def _get_metric_properties_id(self):
        metric_properties = get_properties_obj(bias_property=self.bias_property.value,
                                               dimension_properties=self.dimension_properties_pids,
                                               session=self.db_session,
                                               create_if_no_exist=True)
        return metric_properties.id

    def _get_population_filter(self):
        pop_filter = {PopulationDefinition.ALL_WIKIDATA: None,
                      PopulationDefinition.GTE_ONE_SITELINK: human.sitelink_count > 0,
                      }
        return pop_filter[self.population_definition]

    def _get_dim_cols_from_dim_props(self):
        """
        note this is for a list of props
        """
        col_map = {Properties.PROJECT: human_sitelink.sitelink,
                   Properties.CITIZENSHIP: human_country.country,
                   Properties.DATE_OF_BIRTH: human.year_of_birth,
                   Properties.DATE_OF_DEATH: human.year_of_death,
                   Properties.OCCUPATION: human_occupation.occupation}
        return [col_map[p] for p in self.dimension_properties]

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

    def compile(self):
        # make something approximating
        # metric_q = db_session.query(human.gender, human_sitelink.sitelink, human_country.country,
        #                             func.count(human.gender)) \
        #     .join(human_country, and_(human.qid == human_country.human_id, human.fill_id == human_country.fill_id)) \
        #     .join(human_sitelink, and_(human.qid == human_sitelink.human_id, human.fill_id == human_sitelink.fill_id)) \
        #     .group_by(human_country.country, human_sitelink.sitelink, human.gender)
        bias_col = human.gender  # maybe getattr(human, bias_property.name.lower()
        count_col = func.count(bias_col)
        dim_cols = self._get_dim_cols_from_dim_props()
        metric_cols = [bias_col, *dim_cols, count_col]
        group_bys = [bias_col] + dim_cols
        population_filter = self._get_population_filter()

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

        if population_filter is not None:
            metric_q = metric_q.filter(population_filter)

        # add gender is not null
        metric_q = metric_q.filter(isnot(human.gender, None))

        # current fill filter
        metric_q = metric_q.filter(human.fill_id == self.fill_id)
        metric_q = metric_q.group_by(*group_bys)
        self.metric_q = metric_q

    def execute(self):
        try:
            # self.db_session.rollback()
            self.metric_res = self.metric_q.all()
        except:
            raise

    def _db_save(self, orm_obj_list, method='bulk_save_objects'):
        db_save_fn = getattr(self.db_session, method)
        try:
            db_save_fn(orm_obj_list)
            self.db_session.commit()
        except sqlalchemy.exc.IntegrityError as insert_error:
            if insert_error.orig.args[1].startswith('Duplicate entry'):
                log.info('attempting to add a metric thats already been added')
                pass
            else:
                raise insert_error
        # try:
        #     self.db_session.add_all(self.insert_metrics)
        #     self.db_session.commit()
        # except sqlalchemy.exc.IntegrityError:
        #     # try one by one
        #     for insert_metric in self.insert_metrics:
        #         try:
        #             self.db_session.add(insert_metric)
        #         except sqlalchemy.exc.IntegrityError as ie:
        #             if ie.code == 1062: # duplicate
        #                 log.info(f'duplicate error on {insert_metric}')
        #             else:
        #                 self.db_session.rollback()
        #                 raise

    def persist(self):
        self.aggregation_getter.get_all_known_aggregations_of_props()
        # these remain static
        for row_i, row in enumerate(self.metric_res):
            if row_i % 1000 == 0:
                log.info(row_i)
                save_try_count = 0
                while -1 < save_try_count < 3:
                    save_try_count += 1
                    try:
                        self._db_save(orm_obj_list=self.insert_metrics)
                        save_try_count = -1
                    except sqlalchemy.exc.InvalidRequestError:
                        self.db_session.rollback()
                        time.sleep(1)
                    finally:
                        self.db_session.close()
                if save_try_count == -1:
                    self.insert_metrics = []  # emulating saving in batches of 1000

            # TODO do this by name lookup not positions
            gender = row[0]
            count = row[-1]
            prop_vals = row[1:-1]
            # hope these align, this is why we need to do it by name
            dimension_values = {prop_id: prop_val for prop_id, prop_val in
                                zip(self.dimension_properties_pids, prop_vals)}
            bias_value = {self.bias_property.value: gender}
            try:
                agg_vals_id = self.aggregation_getter.lookup(bias_value=bias_value,
                                                             dimension_values=dimension_values)
            except NoSuchWikiError:
                log.info(f'skipping something that dimension values {dimension_values}')
                continue  # if there's a new wiki, we won't count it
            a_metric = metric(fill_id=self.fill_id,
                              population_id=self.population_definition.value,
                              properties_id=self.metric_properties_id,
                              aggregations_id=agg_vals_id,
                              bias_value=gender,
                              total=count)
            self.insert_metrics.append(a_metric)

        # the last bits
        self._db_save(orm_obj_list=self.insert_metrics)

        return

    def run(self):
        # can do timing here
        self.compile()
        self.execute()
        self.persist()


if __name__ == '__main__':
    create_execute = sys.argv[1] if len(sys.argv) >= 2 else None
    dump_date = sys.argv[2] if len(sys.argv) >= 3 else None
    log.info(f'Specified dump date: {dump_date}')
    mf = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'], fill_date=dump_date)
    if create_execute:
        log.info(f"Attempting to run {create_execute} on metrics factory")
        getattr(mf, create_execute)()

    log.info("Generate metrics---DONE")
    # in the future metricfactor should fan this out to metriccreator via the config
    # mc = MetricCreator(population_definition=PopulationDefinition.GTE_ONE_SITELINK,
    #                    bias_property=Properties.GENDER,
    #                    dimension_properties=[Properties.PROJECT, Properties.CITIZENSHIP],
    #                    fill_id=mf.curr_fill,
    #                    thresholds=None,
    #                    db_session=mf.db_session)
    # mc2 = MetricCreator(population_definition=PopulationDefinition.GTE_ONE_SITELINK,
    #                    bias_property=Properties.GENDER,
    #                    dimension_properties=[Properties.DATE_OF_BIRTH],
    #                    fill_id=mf.curr_fill,
    #                    thresholds=None,
    #                    db_session=mf.db_session)
    # mc.run()
    # log.info(f'Len of metric res is {len(mc.metric_res)}')
