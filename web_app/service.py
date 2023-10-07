from src.store import ApplicationStore, ExecutionStore
from src.models import Application, Source, Transformation, Action, Job
from src.job_executor import JobExecutor
from src.utils import get_logger
import copy


class WebAppConfig:

    def __init__(self, parameters: dict):
        self.parameters = parameters

    def app_config_file(self) -> str:
        return self.parameters.get('app_config_file')

    def execution_summary_dir(self) -> str:
        return self.parameters.get('execution_summary_dir')

    def get_value(self, key: str):
        return self.parameters.get(key)

    def get_value(self, key: str, default):
        return self.parameters.get(key, default)


class APIResponse:

    def __init__(self, status_code, data=[], message: str = None):
        self.status_code = status_code
        self.data = data
        self.message = message

    def to_response(self) -> dict:
        response = {
            'status_code': self.status_code
        }
        if self.data:
            response['data'] = self.data
        if self.message:
            response['message'] = self.message
        return response


class WebAppService:

    def __init__(self):
        self.logger = get_logger()

    def initialize(self, config: WebAppConfig):
        self.config = config
        self.application_store = ApplicationStore(config.app_config_file())
        self.execution_store = ExecutionStore(config.execution_summary_dir())

    def __fetch_job_names(self) -> list:
        active_jobs = list(filter(lambda app: app.status, self.application_store.load_all_jobs()))
        job_data = {}
        for active_job in active_jobs:
            job_data[active_job.name] = active_job.object_id

        return job_data

    def list_all_jobs(self) -> list:
        self.logger.debug("executing : WebAppService.list_all_jobs()")
        jobs = list(map(lambda job: WebAppService.__map_job(job), self.application_store.load_all_jobs()))
        self.logger.debug("exiting : WebAppService.list_all_jobs()")
        if len(jobs) == 0:
            return APIResponse(status_code=204, message="No jobs found").to_response()
        else:
            return APIResponse(status_code=200, data=jobs).to_response()

    def application_details(self, job_name: str):
        self.logger.debug("executing : WebAppService.application_details()")
        job_details = self.__fetch_job_names()
        self.logger.debug(f'job to run - {job_name}')
        if job_name not in job_details.keys():
            self.logger.error(f'job not found - {job_name}')
            return APIResponse(status_code=400, message=f"Application not found for job: {job_name}").to_response()

        job_data = self.application_store.lookup_job(job_id=job_details[job_name])
        application = self.application_store.lookup_application(application_id=job_data.application_id)
        self.logger.debug("exiting : WebAppService.application_details()")
        if application:
            return APIResponse(status_code=200,
                               data=WebAppService.__map_application(application=application)).to_response()
        else:
            return APIResponse(status_code=204, message=f"Application not found for job: {job_name}").to_response()

    def job_details(self, job_name: str):
        self.logger.debug("executing : WebAppService.job_details()")
        job_details = self.__fetch_job_names()
        self.logger.debug(f'job to run - {job_name}')
        if job_name not in job_details.keys():
            self.logger.error(f'job not found - {job_name}')
            return APIResponse(status_code=400, message=f"Job not found: {job_name}").to_response()

        job_data = self.application_store.lookup_job(job_id=job_details[job_name])
        if job_data:
            return APIResponse(status_code=200,
                               data=WebAppService.__map_job(job_data)).to_response()
        else:
            return APIResponse(status_code=204, message=f"Job not found: {job_name}").to_response()

    def run_job(self, job_name: str, job_parameters={}):
        self.logger.debug("executing : WebAppService.run_job()")
        job_details = self.__fetch_job_names()
        self.logger.debug(f'job to run - {job_name}')
        if job_name not in job_details.keys():
            self.logger.error(f'job not found - {job_name}')
            return APIResponse(status_code=400, message=f"Job not found: {job_name}").to_response()

        job_data = self.application_store.lookup_job(job_id=job_details[job_name])
        self.logger.debug(f'job data - {job_data}')
        job_parameters_existing = copy.copy(job_data.job_parameters())
        job_parameters_existing['app_id'] = job_data.object_id
        job_parameters_existing['job_id'] = job_details[job_name]
        job_parameters_existing['submitter'] = 'UI'
        job_parameters_existing['config_file'] = self.config.app_config_file()
        job_parameters_existing['execution_summary_dir'] = self.config.execution_summary_dir()

        for parameter_name in job_parameters.keys():
            job_parameters_existing[parameter_name] = job_parameters[parameter_name]

        job_executor = JobExecutor()
        job_executor.execute_job(job_parameters_existing)

        self.logger.debug("exiting : WebAppService.run_job()")
        return [f"started - {job_name}"]

    def jobs_history(self, job_name: str, is_current=False):
        job_data = self.__fetch_job_names()
        job_id = job_data[job_name]
        if not job_id:
            return APIResponse(status_code=400, message=f"Job not found: {job_name}").to_response()

        all_history = list(map(lambda record: WebAppService.__map_job_history(record),
                               self.execution_store.get_job_history(job_id)))

        if len(all_history) == 0:
            return APIResponse(status_code=204, message=f"Job history not found for job: {job_name}").to_response()

        if is_current:
            return APIResponse(status_code=200,
                               data=all_history[len(all_history) - 1]).to_response()
        else:
            return APIResponse(status_code=200, data=all_history).to_response()

    @staticmethod
    def __map_job_history(job_history):
        if job_history.run_by is None:
            run_by = '-'
        else:
            run_by = job_history.run_by

        if len(job_history.message) > 30:
            message = (job_history.message[:27] + '...')
        else:
            message = job_history.message

        return {"job_id": job_history.job_id,
                "app_id": job_history.app_id,
                "run_by": run_by,
                "status": job_history.status,
                "start_time": job_history.start_time,
                "end_time": job_history.end_time,
                "run_type": job_history.run_type,
                "message": message,
                "metrics": job_history.metrics
                }

    @staticmethod
    def __map_status(status):
        if status:
            return "Active"
        else:
            return "Inactive"

    @staticmethod
    def __map_job(job: Job) -> dict:
        return {"object_id": job.object_id,
                "name": job.name,
                "status": WebAppService.__map_status(job.status),
                "description": job.description,
                "application_id": job.application_id,
                "create_date": (job.create_date, "-")[job.create_date is None],
                "update_date": (job.update_date, "-")[job.update_date is None],
                "created_by": (job.created_by, "-")[job.created_by is None],
                "updated_by": (job.updated_by, "-")[job.updated_by is None],
                "job_parameters": job.job_parameters(),
                "is_scheduled": job.is_scheduled()
                }

    @staticmethod
    def __map_source(source: Source) -> dict:
        return {"object_id": source.object_id,
                "name": source.name,
                "status": WebAppService.__map_status(source.status),
                "description": source.description,
                "type": source.source_type,
                "create_date": (source.create_date, "-")[source.create_date is None],
                "update_date": (source.update_date, "-")[source.update_date is None],
                "created_by": (source.created_by, "-")[source.created_by is None],
                "updated_by": (source.updated_by, "-")[source.updated_by is None]
                }

    @staticmethod
    def __map_transformation(transformation: Transformation) -> dict:
        return {"object_id": transformation.object_id,
                "name": transformation.name,
                "status": WebAppService.__map_status(transformation.status),
                "description": transformation.description,
                "type": transformation.transformation_type,
                "create_date": (transformation.create_date, "-")[transformation.create_date is None],
                "update_date": (transformation.update_date, "-")[transformation.update_date is None],
                "created_by": (transformation.created_by, "-")[transformation.created_by is None],
                "updated_by": (transformation.updated_by, "-")[transformation.updated_by is None]
                }

    @staticmethod
    def __map_action(action: Action) -> dict:
        return {"object_id": action.object_id,
                "name": action.name,
                "status": WebAppService.__map_status(action.status),
                "description": action.description,
                "type": action.action_type,
                "create_date": (action.create_date, "-")[action.create_date is None],
                "update_date": (action.update_date, "-")[action.update_date is None],
                "created_by": (action.created_by, "-")[action.created_by is None],
                "updated_by": (action.updated_by, "-")[action.updated_by is None]
                }

    @staticmethod
    def __map_application(application: Application) -> dict:
        sources = list(map(lambda source: WebAppService.__map_source(source), application.sources))
        transformations = list(map(lambda transformation: WebAppService.__map_transformation(transformation),
                                   application.transformations))
        actions = list(map(lambda action: WebAppService.__map_action(action),
                           application.actions))

        return {"object_id": application.object_id,
                "name": application.name,
                "status": WebAppService.__map_status(application.status),
                "description": application.description,
                "source_count": len(application.sources),
                "transformation_count": len(application.transformations),
                "action_count": len(application.actions),
                "create_date": (application.create_date, "-")[application.create_date is None],
                "update_date": (application.update_date, "-")[application.update_date is None],
                "created_by": (application.created_by, "-")[application.created_by is None],
                "updated_by": (application.updated_by, "-")[application.updated_by is None],
                "sources": sources,
                "transformations": transformations,
                "actions": actions
                }
