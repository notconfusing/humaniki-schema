import os
import subprocess
from datetime import datetime, timedelta

import sqlalchemy

from humaniki_schema import db
from humaniki_schema.generate_metrics import MetricFactory
from humaniki_schema.insert import HumanikiDataInserter
from humaniki_schema.queries import get_latest_fill_id, determine_fill_item, create_new_fill, update_fill_detail, \
    get_exact_fill
from humaniki_schema.utils import read_config_file, make_dump_date_from_str, HUMANIKI_SNAPSHOT_DATE_FMT, \
    is_wikimedia_cloud_dump_format, numeric_part_of_filename
from humaniki_schema.log import get_logger

log = get_logger()


class HumanikiOrchestrator(object):
    def __init__(self, config):
        self.config = read_config_file(config, __file__)
        self.config_insertion = self.config['insertion']
        self.overwrite = self.config_insertion['overwrite'] if 'overwrite' in self.config_insertion else False
        self.frontfill_backfill = os.getenv("HUMANIKI_BACKFILL", "front")
        self.db_session = db.session_factory()
        self.working_fill_date = None
        self.humaniki_override_date = os.getenv("HUMANIKI_OVERRIDE_DATE", None)
        if self.humaniki_override_date is not None:
            self.working_fill_date = make_dump_date_from_str(self.humaniki_override_date)
        self.metrics_factory = None
        self.num_procs = os.getenv("HUMANIKI_NUM_PROCS", 4)
        self.fill_id = None
        log.info("Humaniki Orchestrator intialized")

    def _record_stage_on_fill_item(fun):
        def recorder(self):
            stages = get_exact_fill(self.db_session, self.working_fill_date).detail['stages']
            stage_name = fun.__name__
            stages[stage_name] = True
            update_fill_detail(self.db_session, self.fill_id, 'stages', stages)
            fun(self)
        return recorder

    def frontfill_determine_needs_run_from_remote_fill_dt(self):
        """return true if remote date is newer than what we have"""
        if self.working_fill_date is None:
            # that is it wasn't set yet, maybe by override
            try:
                latest_local_fill_id, latest_local_fill_date = get_latest_fill_id(self.db_session)
            except sqlalchemy.orm.exc.NoResultFound:
                # in the case this is the very first run
                latest_local_fill_date = datetime(2012, 1, 1).date()  # when wikidata first started.

            wd_dir_raw = os.listdir(os.environ['HUMANIKI_DUMP_DIR'])
            # filter out broken links
            wd_dir_ls = [os.path.join(os.environ['HUMANIKI_DUMP_DIR'], p) for p in wd_dir_raw]
            wd_dir_ls_links = [os.path.join(os.environ['HUMANIKI_DUMP_DIR'], os.readlink(l)) for l in wd_dir_ls]
            wd_dir_ls_exists = [f for f in wd_dir_ls_links if os.path.exists(f)]
            # make sure the file is like YYYYMMDD.json.gz
            wd_dir_ls_exists_correct = [f for f in wd_dir_ls_exists if is_wikimedia_cloud_dump_format(f)]
            log.info(f'Existing and correct dump files found were {wd_dir_ls_exists_correct}')
            wd_dir_dts = [make_dump_date_from_str(numeric_part_of_filename(dt_s, basenameittoo=True))
                          for dt_s in wd_dir_ls_exists_correct]
            remote_later_than_local = [fd for fd in wd_dir_dts if fd > latest_local_fill_date]

            if remote_later_than_local:
                log.info(f"Lastest local was {latest_local_fill_date}, and {len(wd_dir_dts)} remote dts later")
                remote_infimum_date = min(remote_later_than_local)
                self.working_fill_date = remote_infimum_date
            # select the remote fill date that's earliest but still greater than local
            else:
                log.info(
                    f"Lastest local was {latest_local_fill_date}, and nothing later from {len(wd_dir_dts)} remote dts")

    @_record_stage_on_fill_item
    def execute_java(self):
        ## subprocess.run waits for rterun
        ## java and jar
        JAVA_BIN = os.getenv("HUMANIKI_JAVA_BIN")
        JAR = os.getenv("HUMANIKI_JAR")
        JAVA_TIMEOUT = timedelta(days=1) / timedelta(seconds=1)
        encoding_arg = '-Dfile.encoding=UTF-8'
        dash_jar_arg = '-jar'

        java_call = [JAVA_BIN, encoding_arg, dash_jar_arg, JAR, self.working_fill_date.strftime(HUMANIKI_SNAPSHOT_DATE_FMT)]
        log.info(f'java call: {" ".join(java_call)}')
        java_run_response = subprocess.run(java_call, timeout=JAVA_TIMEOUT)
        if isinstance(java_run_response, subprocess.CompletedProcess):
            log.info('Java complete')
        else:
            log.exception(java_run_response)
            raise RuntimeError

    def execute_inserter(self):
        dump_subset = os.getenv('HUMANIKI_MAX_HUMANS', None)
        hdi = HumanikiDataInserter(config=os.environ['HUMANIKI_YAML_CONFIG'],
                                   dump_date=self.working_fill_date,
                                   dump_subset=dump_subset,
                                   insert_strategy='infile')
        hdi.run()

    def _get_metrics_factory(self):
        if self.metrics_factory is None:
            self.metrics_factory = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'],
                                                 fill_date=self.working_fill_date)
        else:
            pass

    def execute_create_metric_jobs(self):
        self._get_metrics_factory()
        self.metrics_factory.create()

    def execute_metric_jobs_multi(self):
        METRIC_EXECUTOR_SH = 'execute_metrics.sh'
        this_dir = os.getcwd()
        target_dir = os.path.join(this_dir, '..')
        target_f = os.path.join(target_dir, METRIC_EXECUTOR_SH)
        bash = 'bash'
        out_log_f = os.path.join("logs",
                                 f"metric_executor_{self.working_fill_date.strftime(HUMANIKI_SNAPSHOT_DATE_FMT)}.log")
        num_threads = self.num_procs
        dump_dt = self.working_fill_date.strftime(HUMANIKI_SNAPSHOT_DATE_FMT)
        out_redirector_symb = '>'
        execute_metric_call = [bash, target_f, num_threads, dump_dt, out_redirector_symb, out_log_f]
        log.info(f'Execute metric call is: {" ".join(execute_metric_call)}')
        subprocess.run(execute_metric_call)

    def create_fill_obj(self):
        fill_id, fill_dt = determine_fill_item(self.db_session, self.working_fill_date)
        detection_type = 'explicit' if self.humaniki_override_date else 'dumpfiles_poll'
        create_a_fill = None
        if not fill_id:
            # create
            log.info('No fill exists, going to create')
            create_a_fill = True
        elif self.overwrite:
            # fill exists, shall i overwrite?
            log.info(f'Fill exists, and we want to overwrite')
            update_fill_detail(self.db_session, fill_id, 'active', False)
            create_a_fill = True
        else:
            log.info('Fill exists, but overwrite off')
            create_a_fill = False
            raise AssertionError('Already have a fill for this date and overwriting not explicitly set')

        if create_a_fill:
            a_fill = create_new_fill(self.db_session, self.working_fill_date, detection_type)
            self.fill_id = a_fill.id


    def run(self):
        # determine if need run
        if self.frontfill_backfill == 'front':
            log.info("Fill direction is frontfill")
            self.frontfill_determine_needs_run_from_remote_fill_dt()
        elif self.frontfill_backfill == 'back':
            log.info("Fill direction is backfill")
            raise NotImplementedError
        else:
            raise AssertionError("need a front or backfill")

        # then run if needed
        if self.working_fill_date:
            self.create_fill_obj()
            self.execute_java()
            self.execute_inserter()
            self.execute_create_metric_jobs()
            self.execute_metric_jobs_multi()


if __name__ == '__main__':
    orchestrator = HumanikiOrchestrator(os.environ['HUMANIKI_YAML_CONFIG'])
    orchestrator.run()
