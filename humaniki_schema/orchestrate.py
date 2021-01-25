import os
import subprocess
from datetime import datetime, timedelta

import sqlalchemy

from humaniki_schema import db
from humaniki_schema.generate_metrics import MetricFactory
from humaniki_schema.insert import HumanikiDataInserter
from humaniki_schema.queries import get_latest_fill_id
from humaniki_schema.utils import read_config_file, make_dump_date_from_str, HUMANIKI_SNAPSHOT_DATE_FMT, \
    is_wikimedia_cloud_dump_format
from humaniki_schema.log import get_logger
log = get_logger()



class HumanikiOrchestrator(object):
    def __init__(self, config):
        self.config = read_config_file(config, __file__)
        self.frontfill_backfill = os.getenv("HUMANIKI_BACKFILL", "front")
        self.db_session = db.session_factory()
        self.working_fill_date = None
        self.humaniki_override_date = os.getenv("HUMANIKI_OVERRIDE_DATE", None)
        if self.humaniki_override_date is not None:
            self.working_fill_date = make_dump_date_from_str(self.humaniki_override_date)
        self.metrics_factory = None
        log.info("Humaniki Orchestrator intialized")

    def frontfill_determine_needs_run_from_remote_fill_dt(self):
        """return true if remote date is newer than what we have"""
        if self.working_fill_date is None:
            # that is it wasn't set yet, maybe by override
            try:
                latest_local_fill_id, latest_local_fill_date = get_latest_fill_id(self.db_session)
            except sqlalchemy.orm.exc.NoResultFound:
                # in the case this is the very first run
                latest_local_fill_date = datetime(2012,1,1) # when wikidata first started.

            wd_dir_ls = os.listdir(os.environ['HUMANIKI_DUMP_DIR'])
            # filter out broken links
            wd_dir_ls_exists = [p for p in wd_dir_ls if os.path.exists(os.readlink(os.path.join(os.environ['HUMANIKI_DUMP_DIR'], p)))]
            # make sure the file is like YYYYMMDD.json.gz
            wd_dir_ls_exists_correct = [f for f in wd_dir_ls_exists if is_wikimedia_cloud_dump_format(f)]
            log.info(f'Existing and correct dump files found were {wd_dir_ls_exists_correct}')
            wd_dir_dts = [make_dump_date_from_str(dt_s.split('.json.gz')[0]) for dt_s in wd_dir_ls_exists_correct]
            remote_later_than_local = [fd for fd in wd_dir_dts if fd > latest_local_fill_date]

            if remote_later_than_local:
                log.info(f"Lastest local was {latest_local_fill_date}, and {len(wd_dir_dts)} remote dts later")
                remote_infimum_date = min(remote_later_than_local)
                self.working_fill_date = remote_infimum_date
    # select the remote fill date that's earliest but still greater than local
            else:
                log.info(f"Lastest local was {latest_local_fill_date}, and nothing later from {len(wd_dir_dts)} remote dts")

    def execute_java(self):
        ## subprocess.run waits for rterun
        ## java and jar
        JAVA_BIN = os.getenv("HUMANIKI_JAVA_BIN")
        JAR = os.getenv("HUMANIKI_JAR")
        JAVA_TIMEOUT = timedelta(days=1)/timedelta(seconds=1)
        encoding_arg ='-Dfile.encoding=UTF-8'
        dash_jar_arg = '-jar'

        java_call = [JAVA_BIN, encoding_arg, dash_jar_arg, JAR, self.working_fill_date.strftime('%Y%m%d')]
        log.info(f'java call: {java_call}')
        java_run_response = subprocess.run(java_call,  timeout=JAVA_TIMEOUT)
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
            self.metrics_factory = MetricFactory(config=os.environ['HUMANIKI_YAML_CONFIG'], fill_date=self.working_fill_date)
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
        out_log_f = os.path.join("logs", f"metric_executor_{self.working_fill_date.strftime(HUMANIKI_SNAPSHOT_DATE_FMT)}.log")
        out_redirector_symb = '>'
        execute_metric_call = [bash, target_f, out_redirector_symb, out_log_f]
        subprocess.run(execute_metric_call)

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
            self.execute_java()
            self.execute_inserter()
            self.execute_create_metric_jobs()
            self.execute_metric_jobs_multi()

if __name__ == '__main__':
    orchestrator = HumanikiOrchestrator(os.environ['HUMANIKI_YAML_CONFIG'])
    orchestrator.run()