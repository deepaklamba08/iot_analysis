import datetime
import json
import os
import uuid
from abc import ABC, abstractmethod

from src.models import Application, Source, Transformation, Action, Job, User, SchedulerData
from src.utils import get_logger, replace_placeholders, Constants, replace_variables


class ApplicationStore:

    def __init__(self, config_dir: str, parameters: dict = {}):
        self.config_dir = config_dir
        self.parameters = parameters
        self.logger = get_logger()

        self.applications: dict = None
        self.logger.debug(f'initializing ApplicationStore with config_dir - {self.config_dir}')
        self.records = self.__load_all_records()
        self.__load_applications(records=self.records)
        self.logger.debug(f'number of applications - {len(self.applications)}')

    def lookup_application(self, application_id: str) -> Application:
        self.logger.debug(f'executing : ApplicationStore.lookup_application(application_id : {application_id})')
        self.logger.debug(f'exiting : ApplicationStore.lookup_application()')
        return self.applications.get(application_id)

    def load_all_applications(self) -> list:
        return self.applications.values()

    def get_config(self, name: str):
        raw_data_list = list(
            filter(lambda record: record.get('type', 'config') == 'config' and record.get('name') == name,
                   self.records))
        return raw_data_list[0] if len(raw_data_list) > 0 else None

    def create_application(self, app_config: dict, user: str):
        self.logger.debug('executing : ApplicationStore.create_application()')
        app_id = str(uuid.uuid1())
        create_date = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        app_config['id'] = app_id
        app_config['create_date'] = create_date
        app_config['created_by'] = user
        app_config['type'] = 'application'

        ApplicationStore.__update_values(created_by=user, create_date=create_date,
                                         elements=app_config.get('sources', []))
        ApplicationStore.__update_values(created_by=user, create_date=create_date,
                                         elements=app_config.get('transformations', []))
        ApplicationStore.__update_values(created_by=user, create_date=create_date,
                                         elements=app_config.get('actions', []))

        app = Application(
            object_id=app_id,
            name=app_config['name'],
            status=app_config['status'],
            description=app_config.get('description', ''),
            create_date=datetime.datetime.now().strftime(Constants.DATE_FORMAT),
            created_by=user,
            config=app_config.get('config', {}),
            sources=list(map(lambda source_config: ApplicationStore.__parse_source(source_config),
                             app_config['sources'])),
            transformations=list(map(lambda tr_config: ApplicationStore.__parse_transformation(tr_config),
                                     app_config.get('transformations', []))),
            actions=list(map(lambda action_config: ApplicationStore.__parse_action(action_config),
                             app_config['actions']))
        )
        self.__save_file(file_name=f'app_{app_id}.json', data=[app_config])
        self.records = self.__load_all_records()
        self.__load_applications(records=self.records)
        return app_id

    def __save_file(self, file_name, data):
        file_path = os.path.join(self.config_dir, file_name)
        with open(file_path, 'w') as stream:
            stream.write(json.dumps(data, indent=0))

    @staticmethod
    def __update_values(created_by: str, create_date: str, elements: list):
        for element in elements:
            element['id'] = str(uuid.uuid1())
            element['create_date'] = create_date
            element['created_by'] = created_by

    @staticmethod
    def __read_file(config_file: str, parameters):
        with open(config_file, 'r') as data_stream:
            config_str = replace_placeholders(raw_data='\n'.join(data_stream.readlines()),
                                              parameters=parameters)

            config_str = replace_variables(text=config_str,conf=True,record=False,context=parameters)
            return json.loads(config_str)

    def __load_all_records(self) -> list:
        config_files = [file for file in os.listdir(self.config_dir) if file.endswith('.json')]
        elements = []
        for config_file in config_files:
            config_file_path = os.path.join(self.config_dir, config_file)
            elements.extend(ApplicationStore.__read_file(config_file=config_file_path, parameters=self.parameters))
        return elements

    def __load_applications(self, records: list):
        self.logger.debug('loading all applications')
        self.applications: dict = {}
        raw_data_list = list(filter(lambda record: record.get('type', 'application') == 'application', records))
        for raw_data in raw_data_list:
            app = ApplicationStore.__parse_application(raw_data)
            self.applications[app.object_id] = app

    @staticmethod
    def __validate_fields(config: dict, fields: list, message_prefix: str = ""):
        missing_fields = list(filter(lambda field_name: field_name not in config.keys(), fields))
        if len(missing_fields) > 0:
            raise Exception(f"{message_prefix}Missing fields - {', '.join(missing_fields)}")

    @staticmethod
    def __parse_source(config) -> Source:
        return Source(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            description=config['description'],
            create_date=config.get('create_date'),
            update_date=config.get('update_date'),
            created_by=config.get('created_by'),
            updated_by=config.get('updated_by'),
            source_type=config['type'],
            config=config['config']
        )

    @staticmethod
    def __parse_transformation(config) -> Transformation:
        return Transformation(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            description=config['description'],
            create_date=config.get('create_date'),
            update_date=config.get('update_date'),
            created_by=config.get('created_by'),
            updated_by=config.get('updated_by'),
            transformation_type=config['type'],
            config=config['config']
        )

    @staticmethod
    def __parse_action(config) -> Action:
        return Action(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            description=config['description'],
            create_date=config.get('create_date'),
            update_date=config.get('update_date'),
            created_by=config.get('created_by'),
            updated_by=config.get('updated_by'),
            action_type=config['type'],
            config=config['config']
        )

    @staticmethod
    def __parse_application(config) -> Application:

        return Application(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            sources=list(map(lambda source_config: ApplicationStore.__parse_source(source_config),
                             config['sources'])),
            transformations=list(map(lambda tr_config: ApplicationStore.__parse_transformation(tr_config),
                                     config.get('transformations', []))),
            actions=list(map(lambda action_config: ApplicationStore.__parse_action(action_config),
                             config['actions'])),
            create_date=config.get('create_date'),
            update_date=config.get('update_date'),
            created_by=config.get('created_by'),
            updated_by=config.get('updated_by'),
            description=config['description'],
            config=config['config'])


class JobStore:

    def __init__(self, config_dir: str):
        self.config_dir = config_dir
        self.logger = get_logger()
        self.jobs: dict = None
        self.records = self.__load_all_records()
        self.__load_jobs(records=self.records)

    @staticmethod
    def __read_file(config_file: str):
        with open(config_file, 'r') as data_stream:
            return json.loads('\n'.join(data_stream.readlines()))

    def __load_all_records(self) -> list:
        config_files = [file for file in os.listdir(self.config_dir) if file.endswith('.json')]
        elements = []
        for config_file in config_files:
            config_file_path = os.path.join(self.config_dir, config_file)
            elements.extend(JobStore.__read_file(config_file=config_file_path))
        return elements

    @staticmethod
    def __parse_job(config: dict):
        return Job(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            application_id=config['application_id'],
            application_name=config.get('application_name', '-'),
            create_date=config.get('create_date'),
            update_date=config.get('update_date'),
            created_by=config.get('created_by'),
            updated_by=config.get('updated_by'),
            description=config['description'],
            config=config['config'])

    def __load_jobs(self, records: list):
        self.logger.debug('loading all jobs')
        self.jobs: dict = {}
        raw_data_list = list(filter(lambda record: record.get('type', 'application') == 'job', records))
        for raw_data in raw_data_list:
            job = JobStore.__parse_job(raw_data)
            self.jobs[job.object_id] = job

    def lookup_job(self, job_id: str) -> Job:
        self.logger.debug(f'executing : JobStore.lookup_job(job_id : {job_id})')
        self.logger.debug(f'exiting : JobStore.lookup_job()')
        return self.jobs.get(job_id)

    def load_all_jobs(self) -> list:
        return self.jobs.values()

    def __save_file(self, file_name, data):
        file_path = os.path.join(self.config_dir, file_name)
        with open(file_path, 'w') as stream:
            stream.write(json.dumps(data, indent=0))

    def create_job(self, job_data: dict, user: str):
        self.logger.debug('executing : JobStore.create_job()')
        job_id = str(uuid.uuid1())
        create_date = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        job_data['id'] = job_id
        job_data['create_date'] = create_date
        job_data['created_by'] = user
        job_data['type'] = 'job'
        job = JobStore.__parse_job(job_data)

        self.__save_file(file_name=f'job_{job_id}.json', data=[job_data])
        self.records = self.__load_all_records()
        self.__load_jobs(records=self.records)

        return job_id


class ExecutionDetail:
    __ATTRIBUTES = ["execution_id", "job_id", "app_id", "status", "run_by", "message", "start_time", "end_time",
                    "update_time", "run_type", "parameters", "metrics"]

    def __init__(self, execution_id: str = None, job_id: str = None, app_id: str = None, status: str = None,
                 message: str = None,
                 start_time: str = None,
                 update_time: str = None,
                 end_time: str = None,
                 parameters: dict = None,
                 run_by: str = None,
                 run_type: str = "adhoc",
                 metrics: dict = {}):
        self.execution_id = execution_id
        self.job_id = job_id
        self.app_id = app_id
        self.status = status
        self.message = message
        self.start_time = start_time
        self.update_time = update_time
        self.end_time = end_time
        self.parameters = parameters
        self.run_by = run_by
        self.run_type = run_type
        self.metrics = metrics

    @staticmethod
    def from_dict(data: dict):
        ed = ExecutionDetail()
        for field_name in ExecutionDetail.__ATTRIBUTES:
            ed.__setattr__(field_name, data.get(field_name))
        return ed

    def __get_attribute_value(self, attribute_name: str):
        if attribute_name not in ExecutionDetail.__ATTRIBUTES:
            raise Exception(f'invalid attribute - {attribute_name}')
        return self.__getattribute__(attribute_name)

    def update_attributes(self, **kwargs):
        for field_name in ExecutionDetail.__ATTRIBUTES:
            value = kwargs.get(field_name)
            if value:
                self.__setattr__(field_name, value)

    def get_as_dict(self):
        data = {}
        for field_name in ExecutionDetail.__ATTRIBUTES:
            data[field_name] = self.__get_attribute_value(field_name)

        return data

    def __str__(self):
        return f"[job_id = {self.job_id}]"


class ExecutionStoreBase(ABC):

    def __init__(self, parameters: dict):
        self.parameters = parameters

    @abstractmethod
    def create_summary(self, job_id: str, app_id: str, status: str, message: str, run_by: str,
                       run_type: str, parameters: dict = None) -> str:
        pass

    @abstractmethod
    def update_summary(self, execution_id: str, **kwargs):
        pass

    @abstractmethod
    def get_job_history_by_status(self, statuses: list) -> list:
        pass

    @abstractmethod
    def get_job_history(self, job_id: str) -> list:
        pass


class ExecutionStore(ExecutionStoreBase):
    __SUMMARY_FILE_NAME = "summary.json"
    __DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, parameters: dict):
        self.parameters = parameters
        base_dir = parameters['base_dir']
        self.summary_file = os.path.join(base_dir, ExecutionStore.__SUMMARY_FILE_NAME)
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)

        if not os.path.exists(self.summary_file):
            self.__create_empty_summary_file()

    def __create_empty_summary_file(self):
        with open(self.summary_file, 'w') as stream:
            stream.write('[]')

    def __fetch_all_records(self):
        with open(self.summary_file, 'r') as stream:
            data = json.load(stream)
            records = list(map(lambda record: ExecutionDetail.from_dict(record), data))
            return records

    def __fetch_summary(self, execution_id: str) -> ExecutionDetail:
        records = self.__fetch_all_records()
        matched_records = list(filter(lambda record: record.execution_id == execution_id, records))
        if len(matched_records) > 0:
            return matched_records[0]
        else:
            return None

    def __save_records(self, execution_details: list):
        with open(self.summary_file, 'w') as stream:
            data = list(map(lambda record: record.get_as_dict(), execution_details))
            stream.write(json.dumps(data, indent=0))

    def __save_summary(self, execution_detail: ExecutionDetail, replace_existing=True):
        existing_records = self.__fetch_all_records()
        if replace_existing:
            filtered_records = list(
                filter(lambda record: record.execution_id != execution_detail.execution_id, existing_records))
            filtered_records.append(execution_detail)
            self.__save_records(filtered_records)
        else:
            existing_records.append(execution_detail)
            self.__save_records(existing_records)

    def __update_summary(self, execution_detail: ExecutionDetail):
        records = self.__fetch_all_records()
        matched_records = list(
            filter(lambda record: record.execution_id == execution_detail.execution_id, records))
        if matched_records is None or len(matched_records) == 0:
            raise Exception(f'execution summary not found for id - {execution_detail.execution_id}')

        self.__save_summary(execution_detail=execution_detail, replace_existing=True)

    def create_summary(self, job_id: str, app_id: str, status: str, message: str, run_by: str,
                       run_type: str, parameters: dict = None) -> str:
        execution_id = str(uuid.uuid1())
        execution_detail = ExecutionDetail(execution_id=execution_id, job_id=job_id, app_id=app_id, status=status,
                                           message=message,
                                           start_time=datetime.datetime.now().strftime(Constants.DATE_FORMAT),
                                           parameters=parameters, run_by=run_by)
        self.__save_summary(execution_detail=execution_detail, replace_existing=False)
        return execution_id

    def update_summary(self, execution_id: str, **kwargs):
        existing_summary = self.__fetch_summary(execution_id=execution_id)
        if not existing_summary:
            raise Exception(f'execution summary not found for id - {execution_id}')
        existing_summary.update_attributes(**kwargs)
        existing_summary.update_attributes(
            **{
                'execution_id': execution_id,
                'update_time': datetime.datetime.now().strftime(Constants.DATE_FORMAT),
            })
        self.__update_summary(execution_detail=existing_summary)

    def get_job_history(self, job_id: str) -> list:
        records = self.__fetch_all_records()
        matched_records = list(filter(lambda record: record.job_id == job_id, records))
        matched_records = sorted(matched_records,
                                 key=lambda element: datetime.datetime.strptime(element.start_time,
                                                                                ExecutionStore.__DATE_FORMAT))
        return matched_records

    def get_job_history_by_status(self, statuses: list) -> list:
        records = self.__fetch_all_records()
        matched_records = list(filter(lambda record: record.status in statuses, records))
        matched_records = sorted(matched_records,
                                 key=lambda element: datetime.datetime.strptime(element.start_time,
                                                                                ExecutionStore.__DATE_FORMAT))
        return matched_records


class ExecutionStoreV2(ExecutionStoreBase):
    __SUMMARY_FILE_PREFIX = "summary_"
    __DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, parameters: dict):
        self.parameters = parameters
        self.base_dir = parameters['base_dir']
        if not os.path.exists(self.base_dir):
            os.mkdir(self.base_dir)

    @staticmethod
    def __create_empty_summary_file(summary_file: str):
        with open(summary_file, 'w') as stream:
            stream.write('{}')

    @staticmethod
    def __fetch_all_records(summary_file: str):
        with open(summary_file, 'r') as stream:
            data = json.load(stream)
            return ExecutionDetail.from_dict(data)

    @staticmethod
    def __check_file(summary_file: str):
        if not os.path.exists(summary_file):
            ExecutionStoreV2.__create_empty_summary_file(summary_file)

    def __get_file_path(self, execution_id: str):
        return os.path.join(self.base_dir, f'{ExecutionStoreV2.__SUMMARY_FILE_PREFIX}{execution_id}.json')

    @staticmethod
    def __save_records(summary_file, execution_detail: ExecutionDetail):
        with open(summary_file, 'w') as stream:
            stream.write(json.dumps(execution_detail.get_as_dict(), indent=0))

    @staticmethod
    def __save_summary(summary_file, execution_detail: ExecutionDetail):
        ExecutionStoreV2.__save_records(summary_file=summary_file, execution_detail=execution_detail)

    @staticmethod
    def __update_summary(summary_file, execution_detail: ExecutionDetail):
        ExecutionStoreV2.__save_summary(summary_file=summary_file, execution_detail=execution_detail)

    def create_summary(self, job_id: str, app_id: str, status: str, message: str, run_by: str, run_type: str,
                       parameters: dict = None) -> str:
        execution_id = str(uuid.uuid1())
        summary_file = self.__get_file_path(execution_id)
        ExecutionStoreV2.__check_file(summary_file)
        execution_detail = ExecutionDetail(execution_id=execution_id, job_id=job_id, app_id=app_id, status=status,
                                           message=message,
                                           start_time=datetime.datetime.now().strftime(Constants.DATE_FORMAT),
                                           parameters=parameters, run_by=run_by)

        ExecutionStoreV2.__save_summary(summary_file=summary_file, execution_detail=execution_detail)
        return execution_id

    def __fetch_summary(self, execution_id: str) -> ExecutionDetail:
        summary_file = self.__get_file_path(execution_id)
        record = ExecutionStoreV2.__fetch_all_records(summary_file)
        return record

    def update_summary(self, execution_id: str, **kwargs):
        existing_summary = self.__fetch_summary(execution_id=execution_id)
        if not existing_summary:
            raise Exception(f'execution summary not found for id - {execution_id}')
        existing_summary.update_attributes(**kwargs)
        existing_summary.update_attributes(
            **{
                'execution_id': execution_id,
                'update_time': datetime.datetime.now().strftime(Constants.DATE_FORMAT),
            })
        summary_file = self.__get_file_path(execution_id)
        ExecutionStoreV2.__update_summary(summary_file=summary_file, execution_detail=existing_summary)

    def __get_all_files(self):
        files_with_prefix = [f for f in os.listdir(self.base_dir) if
                             f.startswith(ExecutionStoreV2.__SUMMARY_FILE_PREFIX)
                             and os.path.isfile(os.path.join(self.base_dir, f))]
        return list(map(lambda file_path: os.path.join(self.base_dir, file_path), files_with_prefix))

    def __fetch_all(self, filter_fun):
        files = self.__get_all_files()
        elements = list(map(lambda file: ExecutionStoreV2.__fetch_all_records(summary_file=file), files))
        return list(filter(lambda element: filter_fun(element), elements))

    def get_job_history_by_status(self, statuses: list) -> list:
        elements = self.__fetch_all(filter_fun=lambda record: record.status in statuses)
        elements = sorted(elements,
                          key=lambda element: datetime.datetime.strptime(element.start_time,
                                                                         ExecutionStoreV2.__DATE_FORMAT))
        return elements

    def get_job_history(self, job_id: str) -> list:
        elements = self.__fetch_all(filter_fun=lambda record: record.job_id == job_id)
        elements = sorted(elements,
                          key=lambda element: datetime.datetime.strptime(element.start_time,
                                                                         ExecutionStoreV2.__DATE_FORMAT), reverse=True)
        return elements


class DbExecutionStoreBase(ExecutionStoreBase):
    """
    create table execution_result(
    execution_id VARCHAR(64) not null,
    job_id VARCHAR(64),
    app_id VARCHAR(64),
    status VARCHAR(64),
    run_by VARCHAR(64),
    message VARCHAR(256),
    start_time VARCHAR(64),
    update_time VARCHAR(64),
    end_time VARCHAR(64),
    run_type VARCHAR(64),
    parameters VARCHAR(2048),
    metrics VARCHAR(2048)
    )
    """

    def __init__(self, parameters: dict):
        self.logger = get_logger()
        self.parameters = parameters
        import jaydebeapi
        self.conn = jaydebeapi.connect(jclassname=self.parameters['driver_class_name'],
                                       url=self.parameters['jdbc_url'],
                                       driver_args=self.parameters['driver_args'],
                                       jars=self.parameters['jars'])

    def create_summary(self, job_id: str, app_id: str, status: str, message: str, run_by: str,
                       run_type: str, parameters: dict = None) -> str:
        self.logger.debug('executing : DbExecutionStoreBase.create_summary()')
        execution_id = str(uuid.uuid1())
        start_time = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        update_time = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        query_parameters = [execution_id, job_id, app_id, status, message, run_by, run_type, start_time, update_time,
                            json.dumps(parameters)]
        insert_query = 'insert into execution_result(execution_id,job_id,app_id,status,message,run_by,run_type,start_time,update_time,parameters) values(?,?,?,?,?,?,?,?,?,?)'
        with self.conn.cursor() as curs:
            curs.execute(insert_query, query_parameters)
            self.conn.commit()
        self.logger.debug('exiting : DbExecutionStoreBase.create_summary()')
        return execution_id

    def update_summary(self, execution_id: str, **kwargs):
        self.logger.debug('executing : DbExecutionStoreBase.update_summary()')
        update_part = ', '.join(list(map(lambda key: f'{key} = ?', kwargs.keys())))
        update_part = f'{update_part}, update_time = ?'

        update_query = f'update execution_result set {update_part} where execution_id = ?'
        query_parameters = []
        for key in kwargs.keys():
            if key == 'parameters' or key == 'metrics':
                query_parameters.append(json.dumps(kwargs[key]))
            else:
                query_parameters.append(kwargs[key])
        query_parameters.append(datetime.datetime.now().strftime(Constants.DATE_FORMAT))
        query_parameters.append(execution_id)
        with self.conn.cursor() as curs:
            curs.execute(update_query, query_parameters)
            self.conn.commit()
        self.logger.debug('exiting : DbExecutionStoreBase.update_summary()')

    @staticmethod
    def __map_record(select_sequence, data):
        i = 0
        data_dict = {}
        while i < len(select_sequence):
            if select_sequence[i] == 'parameters' or select_sequence[i] == 'metrics':
                if data[i] is not None:
                    data_dict[select_sequence[i]] = json.loads(data[i])
                else:
                    data_dict[select_sequence[i]] = None
            else:
                data_dict[select_sequence[i]] = data[i]
            i = i + 1

        return ExecutionDetail.from_dict(data_dict)

    def get_job_history(self, job_id: str) -> list:
        self.logger.debug(f'executing : DbExecutionStoreBase.get_job_history(job_id : {job_id})')
        select_sequence = ['execution_id', 'job_id', 'app_id', 'status', 'run_by', 'message', 'start_time',
                           'update_time', 'end_time', 'run_type', 'parameters', 'metrics']
        select_query = f"select {', '.join(select_sequence)} from execution_result where job_id = ?"
        with self.conn.cursor() as curs:
            curs.execute(select_query, [job_id])
            records = list(
                map(lambda record: DbExecutionStoreBase.__map_record(select_sequence, record), curs.fetchall()))

            return records

        self.logger.debug(f'exiting : DbExecutionStoreBase.get_job_history()')

    def get_job_history_by_status(self, statuses: list) -> list:
        self.logger.debug(f'executing : DbExecutionStoreBase.get_job_history_by_status(statuses : {statuses})')
        select_sequence = ['execution_id', 'job_id', 'app_id', 'status', 'run_by', 'message', 'start_time',
                           'update_time', 'end_time', 'run_type', 'parameters', 'metrics']

        select_query = f"select {', '.join(select_sequence)} from execution_result where status in ({','.join(list(map(lambda r: '?', statuses)))})"
        with self.conn.cursor() as curs:
            self.logger.debug(f'executing query - {select_query}')
            curs.execute(select_query, statuses)
            records = list(
                map(lambda record: DbExecutionStoreBase.__map_record(select_sequence, record), curs.fetchall()))

            self.logger.debug(f'exiting : DbExecutionStoreBase.get_job_history_by_status()')
            return records


class ExecutionStoreProvider:

    @staticmethod
    def create_execution_store(parameters: dict) -> ExecutionStoreBase:
        execution_summary_config = parameters['execution_summary']
        if execution_summary_config['type'] == 'file':
            return ExecutionStore({'base_dir': execution_summary_config['execution_summary_dir']})
        elif execution_summary_config['type'] == 'file-v2':
            return ExecutionStoreV2({'base_dir': execution_summary_config['execution_summary_dir']})
        elif execution_summary_config['type'] == 'db':
            return DbExecutionStoreBase(execution_summary_config)
        else:
            raise Exception(f"execution store not supported - f{execution_summary_config['type']}")


class UserRepo(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def lookup(self, user_login: str) -> User:
        pass

    @abstractmethod
    def create(self, user: User):
        pass


class FileUserRepo(UserRepo):

    def __init__(self, data_file: str):
        self.data_file = data_file
        self.users = {}

    def lookup(self, user_login: str) -> User:
        if len(self.users) == 0:
            self.users = self.__fetch_all_records()

        return self.users.get(user_login)

    def create(self, user: User):
        existing_user = self.lookup(user_login=user.user_login)
        if existing_user:
            raise Exception(f'user already exists - {user.user_login}')
        user.create_date = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        user.status = True
        self.users[user.user_login] = user
        self.__save_records(self.users)

    def __fetch_all_records(self):
        with open(self.data_file, 'r') as stream:
            data = json.load(stream)
            records = {record['user_login']: FileUserRepo.__read_user(record) for record in data}
            return records

    @staticmethod
    def __read_user(data: dict) -> User:
        return User(
            user_login=data['user_login'],
            password=data['password'],
            first_name=data.get('first_name', None),
            last_name=data.get('last_name', None),
            create_date=data.get('create_date', None),
            update_date=data.get('update_date', None),
            status=data.get('status', False)
        )

    @staticmethod
    def __map_user(user: User) -> dict:
        return {
            'user_login': user.user_login,
            'password': user.password,
            'first_name': user.first_name if user.first_name else '',
            'last_name': user.last_name if user.last_name else '',
            'create_date': user.create_date if user.create_date else '',
            'update_date': user.update_date if user.update_date else '',
            'status': user.status if user.status else False
        }

    def __save_records(self, elements: list):
        data = list(map(lambda record: FileUserRepo.__map_user(record), elements.values()))
        with open(self.data_file, 'w') as stream:
            stream.write(json.dumps(data, indent=0))


class DbUserRepo(UserRepo):
    """
        create table platform_users(
        first_name VARCHAR(64) not null,
        last_name VARCHAR(64) not null,
        user_login VARCHAR(64) not null,
        password VARCHAR(1024) not null,
        create_date VARCHAR(64),
        update_date VARCHAR(64),
        status boolean
        )
        """

    def __init__(self, parameters: dict):
        self.logger = get_logger()
        self.parameters = parameters
        import jaydebeapi
        self.conn = jaydebeapi.connect(jclassname=self.parameters['driver_class_name'],
                                       url=self.parameters['jdbc_url'],
                                       driver_args=self.parameters['driver_args'],
                                       jars=self.parameters['jars'])

    def lookup(self, user_login: str) -> User:
        self.logger.debug(f'executing : DbUserRepo.lookup()')
        select_sequence = ['first_name', 'last_name', 'user_login', 'password', 'create_date', 'update_date', 'status']
        select_query = f"select {', '.join(select_sequence)} from platform_users where user_login = ?"
        with self.conn.cursor() as curs:
            curs.execute(select_query, [user_login])
            return DbUserRepo.__map_record(select_sequence, curs.fetchone())

        self.logger.debug(f'exiting : DbUserRepo.lookup()')

    def create(self, user: User):
        self.logger.debug('executing : DbUserRepo.create()')
        insert_sequence = ['first_name', 'last_name', 'user_login', 'password', 'create_date', 'update_date', 'status']
        query_parameters = [user.first_name, user.last_name, user.user_login, user.password, user.create_date,
                            user.update_date, user.status]
        insert_query = f'insert into platform_users({", ".join(insert_sequence)}) values({", ".join(list(map(lambda r: "?", insert_sequence)))})'
        with self.conn.cursor() as curs:
            curs.execute(insert_query, query_parameters)
            self.conn.commit()
        self.logger.debug('exiting : DbUserRepo.create()')

    @staticmethod
    def __map_record(select_sequence, data):
        i = 0
        data_dict = {}
        while i < len(select_sequence):
            data_dict[select_sequence[i]] = data[i]
            i = i + 1
        return DbUserRepo.__read_user(data_dict)

    @staticmethod
    def __read_user(data: dict) -> User:
        return User(
            user_login=data['user_login'],
            password=data['password'],
            first_name=data.get('first_name', None),
            last_name=data.get('last_name', None),
            create_date=data.get('create_date', None),
            update_date=data.get('update_date', None),
            status=data.get('status', False)
        )


class UserRepoProvider:

    @staticmethod
    def create_user_repo(parameters: dict) -> UserRepo:
        user_repo_config = parameters['user_repo']
        if user_repo_config['type'] == 'file':
            return FileUserRepo(data_file=user_repo_config['data_file'])
        if user_repo_config['type'] == 'db':
            return DbUserRepo(data_file=user_repo_config)
        else:
            raise Exception(f"user repo not supported - f{user_repo_config['type']}")


class SchedulerRepo(ABC):

    @abstractmethod
    def lookup(self) -> SchedulerData:
        pass

    @abstractmethod
    def get_status(self):
        pass

    @abstractmethod
    def make_action(self, action: str):
        pass


class FileSchedulerRepo(SchedulerRepo):
    def __init__(self, data_file: str):
        self.data_file = data_file
        self.sch_info = None
        self.__refresh_sch_data()

    def __refresh_sch_data(self):
        with open(self.data_file, 'r') as stream:
            data = json.load(stream)
            self.sch_info = FileSchedulerRepo.__read_schedule_data(data)

    @staticmethod
    def __read_schedule_data(data: dict) -> SchedulerData:
        return SchedulerData(
            object_id=data['object_id'],
            name=data['name'],
            description=data.get('description', None),
            status=data['status'],
            current_state=data.get('current_state', 'idle'),
            host=data.get('host','NA'),
            create_date=data.get('create_date', None),
            created_by=data.get('created_by', None),
            config=data.get('config', {})
        )

    @staticmethod
    def __map_schedule_data(sch_data: SchedulerData) -> dict:
        return {
            "object_id": sch_data.object_id if sch_data.object_id else str(uuid.uuid1()),
            "name": sch_data.name,
            "status": sch_data.status,
            "current_state": sch_data.current_state,
            "create_date": sch_data.create_date,
            "description": sch_data.description,
            "created_by": sch_data.created_by,
            "config": sch_data.config
        }

    def __save_records(self, sch_data: SchedulerData):
        with open(self.data_file, 'w') as stream:
            data = FileSchedulerRepo.__map_schedule_data(sch_data)
            stream.write(json.dumps(data, indent=0))

    def lookup(self) -> SchedulerData:
        self.__refresh_sch_data()
        return self.sch_info

    def get_status(self):
        self.__refresh_sch_data()
        return self.sch_info.current_state

    def make_action(self, action: str):

        if action == 'start':
            self.sch_info.current_state = 'running'
        elif action == 'stop':
            self.sch_info.current_state = 'stopped'
        else:
            raise Exception(f'unknown action - {action}')

        self.__save_records(sch_data=self.sch_info)


class DbSchedulerRepo(SchedulerRepo):

    def __init__(self, parameters: dict):
        self.logger = get_logger()
        self.parameters = parameters
        import jaydebeapi
        self.conn = jaydebeapi.connect(jclassname=self.parameters['driver_class_name'],
                                       url=self.parameters['jdbc_url'],
                                       driver_args=self.parameters['driver_args'],
                                       jars=self.parameters['jars'])

    def lookup(self) -> SchedulerData:
        pass

    def get_status(self):
        pass

    def make_action(self, action: str):
        pass


class SchedulerRepoProvider:

    @staticmethod
    def create_scheduler_repo(parameters: dict) -> SchedulerRepo:
        sch_config = parameters['sch_repo']
        if sch_config['type'] == 'file':
            return FileSchedulerRepo(data_file=sch_config['data_file'])
        if sch_config['type'] == 'db':
            return DbSchedulerRepo(data_file=sch_config)
        else:
            raise Exception(f"user repo not supported - f{sch_config['type']}")
