from src.job_executor import JobExecutor, JobExecutorOrchestrator
from src.models import Application, Source, Transformation, Action, Job, User
from src.store import ApplicationStore, ExecutionStoreProvider, JobStore, UserRepoProvider, SchedulerRepoProvider
from src.utils import get_logger
from src.utils import read_config_file


class WebAppConfig:

    def __init__(self, parameters: dict):
        self.parameters = parameters

    def analysis_app_config(self) -> str:
        return self.parameters.get('analysis_app_config')

    def get_value(self, key: str):
        return self.parameters.get(key)

    def get_property(self, key: str):
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

    def __init__(self, config: WebAppConfig):
        self.logger = get_logger()
        self.config = config
        self.analysis_app_config = read_config_file(config.analysis_app_config())['app']
        self.application_store = ApplicationStore(self.analysis_app_config['app_config_file'])
        self.job_store = JobStore(self.analysis_app_config['app_config_file'])
        self.execution_store = ExecutionStoreProvider.create_execution_store(self.analysis_app_config)
        self.sch_repo = SchedulerRepoProvider.create_scheduler_repo(self.analysis_app_config)
        self.job_exe_orch = JobExecutorOrchestrator(
            sch_repo=self.sch_repo,
            executor=JobExecutor(config.analysis_app_config())
        )
        self.user_repo = UserRepoProvider.create_user_repo(self.analysis_app_config)

    def __fetch_job_names(self) -> list:
        active_jobs = list(filter(lambda app: app.status, self.job_store.load_all_jobs()))
        job_data = {}
        for active_job in active_jobs:
            job_data[active_job.name] = active_job.object_id

        return job_data

    def status(self) -> str:
        self.logger.debug("executing : WebAppService.status()")
        return "running"

    def list_all_jobs(self) -> list:
        self.logger.debug("executing : WebAppService.list_all_jobs()")
        jobs = list(map(lambda job: WebAppService.__map_job(job), self.job_store.load_all_jobs()))
        self.logger.debug("exiting : WebAppService.list_all_jobs()")
        if len(jobs) == 0:
            return APIResponse(status_code=204, message="No jobs found").to_response()
        else:
            return APIResponse(status_code=200, data=jobs).to_response()

    def application_details(self, job_name: str):
        self.logger.debug("executing : WebAppService.application_details()")
        job_details = self.__fetch_job_names()
        self.logger.debug(f'job name - {job_name}')
        if job_name not in job_details.keys():
            self.logger.error(f'job not found - {job_name}')
            return APIResponse(status_code=400, message=f"Application not found for job: {job_name}").to_response()

        job_data = self.job_store.lookup_job(job_id=job_details[job_name])
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

        job_data = self.job_store.lookup_job(job_id=job_details[job_name])
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
        self.job_exe_orch.schedule_job(
            job_id=job_details[job_name], submitter='UI',
            parameters=job_parameters
        )

        self.logger.debug("exiting : WebAppService.run_job()")
        return [f"started - {job_name}"]

    def jobs_history(self, job_name: str, is_current=False):
        self.logger.debug(f"executing : WebAppService.jobs_history(job_name : {job_name}, is_current : {is_current})")
        job_data = self.__fetch_job_names()
        job_id = job_data.get(job_name)
        if not job_id:
            return APIResponse(status_code=400, message=f"Job not found: {job_name}").to_response()
        all_history = list(map(lambda record: WebAppService.__map_job_history(record),
                               self.execution_store.get_job_history(job_id=job_id)))

        if len(all_history) == 0:
            return APIResponse(status_code=204, message=f"Job history not found for job: {job_name}").to_response()

        if is_current:
            return APIResponse(status_code=200,
                               data=all_history[0]).to_response()
        else:
            return APIResponse(status_code=200, data=all_history).to_response()

    def get_user(self, login: str) -> User:
        self.logger.debug("executing : WebAppService.get_user()")
        return self.user_repo.lookup(user_login=login)

    def create_user(self, login: str, password: str, first_name: str, last_name: str):
        self.logger.debug("executing : WebAppService.create_user()")
        self.user_repo.create(
            User(user_login=login, password=password, first_name=first_name, last_name=last_name,
                 create_date=None, status=True))

    def executor_list(self):
        self.logger.debug("executing : WebAppService.executor_list()")
        executors = self.sch_repo.list_all()
        executors = list(
            map(lambda element: WebAppService.__map_executor_info(element), executors))
        if executors:
            return APIResponse(status_code=200, data=executors).to_response()
        else:
            return APIResponse(status_code=204, message="Executor info not found").to_response()

    def executor_info(self, sch_name: str):
        self.logger.debug("executing : WebAppService.executor_info()")
        executor_info = self.sch_repo.lookup(sch_name=sch_name)
        if executor_info:
            return APIResponse(status_code=200, data=WebAppService.__map_executor_info(executor_info)).to_response()
        else:
            return APIResponse(status_code=204, message="Executor info not found").to_response()

    def executor_action(self, sch_name: str, action: str):
        self.logger.debug("executing : WebAppService.executor_action()")
        executor_info = self.sch_repo.lookup(sch_name=sch_name)
        if not executor_info:
            return APIResponse(status_code=204, message="Executor info not found").to_response()

        curr_status = executor_info.current_state
        if curr_status == 'running' and action == 'start':
            self.logger.debug('Job executor is already running')
            return APIResponse(status_code=200, message="Job executor is already running").to_response()
        elif curr_status == 'running' and action == 'stop':
            self.logger.debug('Job executor is running, stopping now')
            return APIResponse(status_code=200, message="Job executor is running, stopping now").to_response()
        elif curr_status == 'stopped' and action == 'start':
            self.logger.debug('Starting job executor now')
            return APIResponse(status_code=200, message="Starting job executor now").to_response()
        elif curr_status == 'stopped' and action == 'stop':
            self.logger.debug('Job executor is already stopped')
            return APIResponse(status_code=200, message="Job executor is already stopped").to_response()
        else:
            return APIResponse(status_code=400, message="Job executor status is invalid").to_response()


    def get_element_config(self, element: str):
        self.logger.debug(f"executing : WebAppService.get_element_config(element: {element})")
        config = self.application_store.get_config(name=element)
        return APIResponse(status_code=200, data=config).to_response() if config else APIResponse(
            status_code=404).to_response()

    def create_application(self, app_data: dict, user: str):
        try:

            self.application_store.create_application(app_config=app_data, user=user)
            self.logger.info("Application created successfully")
            return APIResponse(status_code=200, message="Application created successfully").to_response()
        except Exception as ex:
            self.logger.error(f"Error occurred: {ex}")
            return APIResponse(status_code=500, message="An unexpected error occurred").to_response()

    def create_job(self, job_data: dict, user: str):
        try:
            self.job_store.create_job(job_data=job_data, user=user)
            self.logger.info("Job created successfully")
            return APIResponse(status_code=200, message="Job created successfully").to_response()
        except Exception as ex:
            self.logger.error(f"Error occurred: {ex}")
            return APIResponse(status_code=500, message="An unexpected error occurred").to_response()

    def get_app_names(self):
        self.logger.debug(f"executing : WebAppService.logger()")
        app_dict = {app.name: app.object_id for app in self.application_store.load_all_applications()}
        return APIResponse(status_code=200, data=app_dict).to_response()

    def get_application_details(self, app_id: str):
        self.logger.debug(f"executing : WebAppService.get_application_details(app_id : {app_id})")
        application = self.application_store.lookup_application(application_id=app_id)
        self.logger.debug("exiting : WebAppService.application_details()")
        if application:
            return APIResponse(status_code=200,
                               data=WebAppService.__map_application(application=application)).to_response()
        else:
            return APIResponse(status_code=204, message=f"Application not found : {app_id}").to_response()

    @staticmethod
    def __map_executor_info(executor_info):
        return {
            "name": executor_info.name,
            "object_id": executor_info.object_id,
            "status": WebAppService.__map_status(executor_info.status),
            "create_date": (executor_info.create_date, "-")[executor_info.create_date is None],
            "created_by": (executor_info.created_by, "-")[executor_info.created_by is None],
            "description": executor_info.description,
            "config": executor_info.config,
            "current_state": executor_info.current_state,
            "host": executor_info.host
        }

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

        return {
            "execution_id": job_history.execution_id,
            "job_id": job_history.job_id,
            "app_id": job_history.app_id,
            "app_name": job_history.app_id,
            "run_by": run_by,
            "status": job_history.status,
            "start_time": job_history.start_time,
            "end_time": job_history.end_time if job_history.end_time else '-',
            "run_type": job_history.run_type if job_history.run_type else '-',
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
                "application_name": job.application_name,
                "create_date": (job.create_date, "-")[job.create_date is None],
                "update_date": (job.update_date, "-")[job.update_date is None],
                "created_by": (job.created_by, "-")[job.created_by is None],
                "updated_by": (job.updated_by, "-")[job.updated_by is None],
                "job_parameters": job.job_parameters(),
                "is_scheduled": "Yes" if job.is_scheduled() else "No",
                "scheduler_expression": job.scheduler_expression() if job.scheduler_expression() else '-',
                "scheduler_name": job.scheduler_name
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
                "updated_by": (source.updated_by, "-")[source.updated_by is None],
                "type": source.source_type
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
                "updated_by": (transformation.updated_by, "-")[transformation.updated_by is None],
                "type": transformation.transformation_type
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
                "updated_by": (action.updated_by, "-")[action.updated_by is None],
                "type": action.action_type
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
                "actions": actions,
                "config": application.config
                }
