from src.models import Application, Source, Transformation, Action, Job
import json
from src.utils import get_logger, replace_placeholders, Constants
import os
import datetime
import uuid
from abc import ABC, abstractmethod
import jaydebeapi


class ApplicationStore:

    def __init__(self, config_file: str, parameters: dict = {}):
        self.config_file = config_file
        self.parameters = parameters
        self.logger = get_logger()

        self.applications: dict = None
        self.jobs: dict = None
        records = self.__load_all_records()
        self.__load_applications(records=records)
        self.__load_jobs(records=records)
        self.logger.debug(f'number of applications - {len(self.applications)}')

    def lookup_application(self, application_id: str) -> Application:
        self.logger.debug(f'executing : ApplicationStore.lookup_application(application_id : {application_id})')
        self.logger.debug(f'exiting : ApplicationStore.lookup_application()')
        return self.applications.get(application_id)

    def load_all_applications(self) -> list:
        return self.applications.values()

    def lookup_job(self, job_id: str) -> Job:
        self.logger.debug(f'executing : ApplicationStore.lookup_job(job_id : {job_id})')
        self.logger.debug(f'exiting : ApplicationStore.lookup_job()')
        return self.jobs.get(job_id)

    def load_all_jobs(self) -> list:
        return self.jobs.values()

    def __load_all_records(self) -> list:
        if not self.config_file:
            raise Exception(f'config file is invalid - {self.config_file}')
        with open(self.config_file, 'r') as data_stream:
            config_str = replace_placeholders(raw_data='\n'.join(data_stream.readlines()),
                                              parameters=self.parameters)
            return json.loads(config_str)

    def __load_applications(self, records: list):
        self.logger.debug('loading all applications')
        self.applications: dict = {}
        raw_data_list = list(filter(lambda record: record.get('type', 'application') == 'application', records))
        for raw_data in raw_data_list:
            app = ApplicationStore.__parse_application(raw_data)
            self.applications[app.object_id] = app

    @staticmethod
    def __parse_job(config: dict):
        return Job(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            application_id=config['application_id'],
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
            job = ApplicationStore.__parse_job(raw_data)
            self.jobs[job.object_id] = job

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
    def get_job_history(self, job_id: str) -> list:
        pass


class ExecutionStore(ExecutionStoreBase):
    __SUMMARY_FILE_NAME = "summary.json"

    def __init__(self, parameters: dict):
        self.parameters = parameters
        base_dir = parameters['base_dir']
        self.summary_file = os.path.join(base_dir, ExecutionStore.__SUMMARY_FILE_NAME)
        if not os.path.exists(base_dir):
            os.mkdir(base_dir)

        self.__get_summary_file_name(base_dir)
        if not os.path.exists(self.summary_file):
            self.__create_empty_summary_file()

    def __get_summary_file_name(self, base_dir: str):
        summary_files = list(filter(lambda file_path: file_path.startswith("summary_"), os.listdir(base_dir)))

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
        return matched_records


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
        self.parameters = parameters
        self.conn = jaydebeapi.connect(jclassname=self.parameters['driver_class_name'],
                                       url=self.parameters['jdbc_url'],
                                       driver_args=self.parameters['driver_args'],
                                       jars=self.parameters['jars'])

    def create_summary(self, job_id: str, app_id: str, status: str, message: str, run_by: str,
                       run_type: str, parameters: dict = None) -> str:
        execution_id = str(uuid.uuid1())
        start_time = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        update_time = datetime.datetime.now().strftime(Constants.DATE_FORMAT)
        query_parameters = [execution_id, job_id, app_id, status, message, run_by, run_type, start_time, update_time,
                            json.dumps(parameters)]
        insert_query = 'insert into execution_result(execution_id,job_id,app_id,status,message,run_by,run_type,start_time,update_time,parameters) values(?,?,?,?,?,?,?,?,?,?)'
        with self.conn.cursor() as curs:
            curs.execute(insert_query, query_parameters)
            self.conn.commit()
        return execution_id

    def update_summary(self, execution_id: str, **kwargs):
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

    @staticmethod
    def __map_record(select_sequence, data):
        i = 0
        data_dict = {}
        while i < len(select_sequence):
            data_dict[select_sequence[i]] = data[i]
            i = i + 1

        return ExecutionDetail.from_dict(data_dict)

    def get_job_history(self, job_id: str) -> list:
        select_sequence = ['execution_id', 'job_id', 'app_id', 'status', 'run_by', 'message', 'start_time',
                           'update_time', 'end_time', 'run_type', 'parameters', 'metrics']
        select_query = f"select {', '.join(select_sequence)} from execution_result where job_id = ?"
        with self.conn.cursor() as curs:
            curs.execute(select_query, [job_id])
            records = list(
                map(lambda record: DbExecutionStoreBase.__map_record(select_sequence, record), curs.fetchall()))

            return records
