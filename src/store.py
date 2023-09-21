from src.models import Application, Source, Transformation, Action
import json
from src.utils import get_logger, replace_placeholders
import os
import datetime
import uuid


class ApplicationStore:

    def __init__(self, config_file: str, parameters: dict = {}):
        self.config_file = config_file
        self.parameters = parameters
        self.applications: dict = None
        self.logger = get_logger()

    def lookup_application(self, application_id: str) -> Application:
        self.logger.debug(f'executing : ApplicationStore.lookup_application(application_id : {application_id})')
        if not self.applications:
            self.logger.debug('loading all applications')
            self.__load_applications()
            self.logger.debug(f'number of applications - {len(self.applications)}')
        self.logger.debug(f'exiting : ApplicationStore.lookup_application()')
        return self.applications.get(application_id)

    def __load_applications(self):
        if not self.config_file:
            raise Exception(f'config file is invalid - {self.config_file}')
        self.applications: dict = {}
        with open(self.config_file, 'r') as data_stream:
            config_str = replace_placeholders(raw_data='\n'.join(data_stream.readlines()),
                                              parameters=self.parameters)
            raw_data_list = json.loads(config_str)
            for raw_data in raw_data_list:
                app = ApplicationStore.__parse_application(raw_data)
                self.applications[app.object_id] = app

    @staticmethod
    def __parse_source(config) -> Source:
        return Source(
            object_id=config['id'],
            name=config['name'],
            status=config['status'],
            description=config['description'],
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
            description=config['description'],
            config=config['config'])


class ExecutionDetail:
    __ATTRIBUTES = ["execution_id", "app_id", "status", "message", "start_time", "end_time", "parameters"]

    def __init__(self, execution_id: str = None, app_id: str = None, status: str = None, message: str = None,
                 start_time: str = None, end_time: str = None,
                 parameters: dict = None):
        self.execution_id = execution_id
        self.app_id = app_id
        self.status = status
        self.message = message
        self.start_time = start_time
        self.end_time = end_time
        self.parameters = parameters

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
        return f"[app_id = {self.app_id}]"


class ExecutionStore:
    __SUMMARY_FILE_NAME = "summary.json"
    __DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, base_dir: str):
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

    def create_summary(self, app_id: str, status: str, message: str, parameters: dict = None) -> str:
        execution_id = str(uuid.uuid1())
        execution_detail = ExecutionDetail(execution_id=execution_id, app_id=app_id, status=status, message=message,
                                           start_time=datetime.datetime.now().strftime(ExecutionStore.__DATE_FORMAT),
                                           parameters=parameters)
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
                'end_time': datetime.datetime.now().strftime(ExecutionStore.__DATE_FORMAT)
            })
        self.__update_summary(execution_detail=existing_summary)
