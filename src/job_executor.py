from src.processor import Orchestrator
from src.models import RuntimeContext
from src.store import ApplicationStore, ExecutionStoreProvider, JobStore
from src.utils import get_logger,read_config_file


class JobExecutor:

    def __init__(self, config_file: str):
        self.logger = get_logger()
        self.config_file = config_file
        yaml_config = read_config_file(self.config_file)
        self.app_config = yaml_config['app']
        self.job_store = JobStore(self.app_config['app_config_file'])
        self.execution_store = ExecutionStoreProvider.create_execution_store(self.app_config)

    def create_runtime_context(self, parameters) -> RuntimeContext:
        import copy
        app_config = copy.copy(self.app_config)
        for key in parameters:
            app_config[key] = parameters[key]
        return RuntimeContext(app_config)

    def execute_jobs(self):
        self.logger.debug('executing : JobExecutor.execute_job()')
        jobs_to_run = self.execution_store.get_job_history_by_status(statuses=['scheduled'])

        self.logger.debug(f'no of scheduled jobs - {len(jobs_to_run)}')
        if len(jobs_to_run) > 0:
            for scheduled_job in jobs_to_run:
                import json
                runtime_context = self.create_runtime_context(json.loads(scheduled_job.parameters))
                application_store = ApplicationStore(runtime_context.config_file(), runtime_context.parameters)
                application = application_store.lookup_application(scheduled_job.app_id)
                if not application:
                    self.logger.error(f'application not found by id - {scheduled_job.app_id}')
                    raise Exception(f'application not found by id - {scheduled_job.app_id}')

                Orchestrator(application_store=application_store,
                             execution_store=self.execution_store,
                             job_store=self.job_store).run_scheduled_application(
                    execution_id=scheduled_job.execution_id,
                    application=application,
                    context=runtime_context)
        else:
            self.logger.debug(f'no jobs to run')

        self.logger.debug('exiting : JobExecutor.execute_job()')

    @staticmethod
    def __merge_parameters(job_parameters, parameters):
        for parameter_name in parameters.keys():
            job_parameters[parameter_name] = parameters[parameter_name]

        return job_parameters

    def schedule_job(self, job_id: str, submitter: str = '-', run_type: str = '-', parameters: dict = {}):
        self.logger.debug('executing : JobExecutor.execute_job()')
        job = self.job_store.lookup_job(job_id)
        if not job:
            self.logger.error(f'job not found by id - {job_id}')
            raise Exception(f'job not found by id - {job_id}')

        job_parameters = JobExecutor.__merge_parameters(job_parameters=job.job_parameters(), parameters=parameters)
        runtime_context = self.create_runtime_context(parameters=job_parameters)
        application_store = ApplicationStore(runtime_context.config_file(), runtime_context.parameters)
        orchestrator = Orchestrator(application_store=application_store,
                                    execution_store=self.execution_store,
                                    job_store=self.job_store)

        orchestrator.schedule_job(job=job, submitter=submitter, run_type=run_type, parameters=job_parameters)
        self.logger.debug('exiting : JobExecutor.execute_job()')
