import os
import sys

if 'PATH_TO_ANALYSIS_APP' in os.environ.keys():
    sys.path.append(os.environ['PATH_TO_ANALYSIS_APP'])

from src.processor import Orchestrator
from src.models import RuntimeContext
from src.store import ApplicationStore, ExecutionStoreProvider, JobStore, SchedulerRepo, SchedulerRepoProvider, \
    ExecutionStoreBase
from src.utils import get_logger, read_config_file


class JobExecutor:

    def __init__(self, app_config, job_store: JobStore, execution_store: ExecutionStoreBase, sch_repo: SchedulerRepo):
        self.logger = get_logger()
        self.app_config = app_config
        self.job_store = job_store
        self.execution_store = execution_store
        self.sch_repo = sch_repo

    def create_runtime_context(self, parameters) -> RuntimeContext:
        import copy
        app_config = copy.copy(self.app_config)
        for key in parameters:
            app_config[key] = parameters[key]
        return RuntimeContext(app_config)

    def execute_jobs(self):
        self.logger.debug('executing : JobExecutor.execute_job()')
        # sch_data = self.sch_repo.lookup()
        #
        # if not sch_data:
        #     raise Exception("Scheduler data not found. Please ensure the scheduler is initialized.")
        #
        # if sch_data.current_state != "running":
        #     self.logger.debug(f"Scheduler is not running. Current status: {sch_data.current_state}")
        #     return

        jobs_to_run = self.execution_store.get_job_history_by_status(statuses=['Scheduled'])

        self.logger.debug(f'no of scheduled jobs - {len(jobs_to_run)}')
        if len(jobs_to_run) > 0:
            for scheduled_job in jobs_to_run:
                runtime_context = self.create_runtime_context(scheduled_job.parameters)
                application_store = ApplicationStore(runtime_context.config_file(), runtime_context.parameters)
                application = application_store.lookup_application(scheduled_job.app_id)
                self.logger.debug(f'loaded application - {application.name}')
                if not application:
                    self.logger.error(f'application not found by id - {scheduled_job.app_id}')

                job_data = self.job_store.lookup_job(job_id=scheduled_job.job_id)
                failure_retry = job_data.get_config_value(key='failure_retry', default='0')
                failure_retry = int(failure_retry)
                retry_interval = job_data.get_config_value(key='failure_retry_interval', default='0')
                retry_interval = int(retry_interval)

                attempt = 0
                while attempt <= failure_retry:
                    result = Orchestrator(application_store=application_store,
                                          execution_store=self.execution_store,
                                          job_store=self.job_store).run_scheduled_application(
                        execution_id=scheduled_job.execution_id,
                        application=application,
                        context=runtime_context)
                    if result.status:
                        break
                    attempt = attempt + 1
                    if retry_interval > 0:
                        self.logger.debug(f'waiting for {retry_interval} seconds before retrying...')
                        import time
                        time.sleep(retry_interval)

                self.logger.debug(f'App execution id - {result.execution_id}, status - {result.status}')
        else:
            self.logger.debug(f'no jobs to run')

        self.logger.debug('exiting : JobExecutor.execute_job()')

    @staticmethod
    def __merge_parameters(job_parameters: list, runtime_parameters: dict):
        run_parameters = {job_parameter.parameter_name: job_parameter.parameter_value for job_parameter in
                          job_parameters}
        for parameter_name in runtime_parameters.keys():
            run_parameters[parameter_name] = runtime_parameters[parameter_name]

        return run_parameters

    def schedule_job(self, job_id: str, submitter: str = '-', run_type: str = '-', parameters: dict = {}):
        self.logger.debug('executing : JobExecutor.execute_job()')
        job = self.job_store.lookup_job(job_id)
        if not job:
            self.logger.error(f'job not found by id - {job_id}')
            raise Exception(f'job not found by id - {job_id}')

        job_parameters = JobExecutor.__merge_parameters(job_parameters=job.job_parameters,
                                                        runtime_parameters=parameters)
        runtime_context = self.create_runtime_context(parameters=job_parameters)
        application_store = ApplicationStore(runtime_context.config_file(), runtime_context.parameters)
        orchestrator = Orchestrator(application_store=application_store,
                                    execution_store=self.execution_store,
                                    job_store=self.job_store)

        orchestrator.schedule_job(job=job, submitter=submitter, run_type=run_type, parameters=job_parameters)
        self.logger.debug('exiting : JobExecutor.execute_job()')


class JobExecutorOrchestrator:

    def __init__(self, sch_repo: SchedulerRepo, executor: JobExecutor):
        self.logger = get_logger()
        self.sch_repo = sch_repo
        self.executor = executor

    def current_status(self):
        self.logger.debug('executing : JobExecutorOrchestrator.current_status()')
        return self.sch_repo.get_status()

    def start(self):
        self.logger.debug('executing : JobExecutorOrchestrator.start()')

        curr_status = self.current_status()
        if curr_status == 'running':
            self.logger.debug('Job executor is already running')
            return
        elif curr_status == 'stopped':
            self.logger.debug('Job executor is stopped, starting now')
            self.sch_repo.make_action(action='start')
        else:
            raise Exception(f'Invalid status - {curr_status}')

    def stop(self):
        self.logger.debug('executing : JobExecutorOrchestrator.stop()')

        curr_status = self.current_status()
        if curr_status == 'running':
            self.logger.debug('Job executor is running, stopping now')
            self.sch_repo.make_action(action='stop')
        elif curr_status == 'stopped':
            self.logger.debug('Job executor is already stopped')
            return
        else:
            raise Exception(f'Invalid status - {curr_status}')

    def schedule_job(self, job_id: str, submitter: str = '-', run_type: str = '-', parameters: dict = {}):
        self.logger.debug('executing : JobExecutorOrchestrator.schedule_job()')
        self.executor.schedule_job(job_id=job_id, submitter=submitter, run_type=run_type, parameters=parameters)

    def get_executor_info(self):
        self.logger.debug('executing : JobExecutorOrchestrator.get_executor_info()')
        executor_info = self.sch_repo.lookup()
        self.logger.debug('exiting : JobExecutorOrchestrator.get_executor_info()')
        return executor_info


if __name__ == '__main__':
    arguments = sys.argv
    arguments = arguments[1:]
    if len(arguments) == 0 or len(arguments) % 2 != 0:
        raise Exception('invalid arguments')

    app_arguments = {arguments[i]: arguments[i + 1] for i in range(0, len(arguments), 2)}
    if 'config_file' not in app_arguments.keys():
        raise Exception('config_file not provided in parameters')

    _config_file = app_arguments['config_file']
    _yaml_config = read_config_file(_config_file)
    _app_config = _yaml_config['app']
    _job_store = JobStore(_app_config['app_config_file'])
    _execution_store = ExecutionStoreProvider.create_execution_store(_app_config)
    _sch_repo = SchedulerRepoProvider.create_scheduler_repo(_app_config)

    job_executor = JobExecutor(_app_config, _job_store, _execution_store, _sch_repo)
    job_executor.execute_jobs()
