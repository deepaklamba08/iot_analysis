import csv
import json
import os
from abc import abstractmethod
from datetime import datetime

from src.models import DataBag, ActionTemplate, DatabagLookup
from src.utils import get_logger


class LogDataAction(ActionTemplate):

    def __init__(self, databag_lookup : DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    @staticmethod
    def __log_data(data: dict):
        if data:
            for key in data:
                LogDataAction.__log_databag(data[key])

    @staticmethod
    def __log_databag(databag: DataBag):
        print('-----------------------------------------------------------')
        print(databag)
        print(f'metadata - {databag.metadata}')
        print(f'data - {databag.data}')

    def call(self, **kwargs):
        self.logger.debug('executing : LogDataAction.call()')
        sources_log = kwargs.get('sources_to_log')
        tr_log = kwargs.get('transformation_to_log')

        if sources_log is None and tr_log is None:
            LogDataAction.__log_data(self.databag_lookup.all_sources_databags())
            LogDataAction.__log_data(self.databag_lookup.all_transformation_databags())
        elif sources_log is not None:
            for sources_name in sources_log:
                source_data = self.databag_lookup.get_databag(name=sources_name, is_source=True)
                LogDataAction.__log_databag(source_data)
        elif tr_log is not None:
            for target_name in tr_log:
                target_data = self.databag_lookup.get_databag(name=target_name, is_source=False)
                LogDataAction.__log_databag(target_data)
        else:
            pass

        self.logger.debug('exiting : LogDataAction.call()')


class DataSinkBaseAction(ActionTemplate):

    def __init__(self, databag_lookup : DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    @abstractmethod
    def get_file_extension(self) -> str:
        pass

    @abstractmethod
    def sink(self, parameters: dict, databag: DataBag, file_path: str):
        pass

    def __get_file_path(self, parameters: dict) -> str:
        file_name = parameters['file_name']
        return os.path.join(parameters['file_dir'],
                            f'{file_name}_{int(datetime.now().timestamp() * 1000000)}.{self.get_file_extension()}')

    def call(self, **kwargs):
        self.logger.debug('executing : DataSinkBaseAction.call()')
        source_type = kwargs.get('source_type')
        source_name = kwargs.get('source_name')

        if source_type == 'source':
            databag = self.databag_lookup.get_databag(name=source_name, is_source=True)
        elif source_type == 'transformation':
            databag = self.databag_lookup.get_databag(name=source_name, is_source=False)
        else:
            raise Exception(f'invalid source_type - {source_type}')
        write_mode = kwargs.get('save_mode', 'overwrite')
        self.logger.info(f'write mode is ser to - {write_mode}')
        file_dir = kwargs['file_dir']
        file_path = self.__get_file_path(kwargs)
        self.logger.info(f'file path - {file_path}')
        if write_mode == 'overwrite':
            if os.path.exists(file_dir):
                import shutil
                shutil.rmtree(file_dir)
                os.mkdir(file_dir)
            else:
                os.mkdir(file_dir)
            self.sink(parameters=kwargs, databag=databag, file_path=file_path)
        elif write_mode == 'append':
            if not os.path.exists(file_dir):
                os.mkdir(file_dir)
            self.sink(parameters=kwargs, databag=databag, file_path=file_path)
        else:
            raise Exception(f'save mode not supported - {write_mode}')

        self.logger.debug('executing : DataSinkBaseAction.call()')


class JsonSinkAction(DataSinkBaseAction):

    def __init__(self, databag_lookup : DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def get_file_extension(self) -> str:
        return 'json'

    def sink(self, parameters: dict, databag: DataBag, file_path: str):
        self.logger.debug('executing : JsonSinkAction.sink()')
        json_object = json.dumps(databag.data, indent=4)

        with open(file=file_path, mode='w') as outfile:
            outfile.write(json_object)
        self.logger.debug('executing : JsonSinkAction.sink()')


class CSVSinkAction(DataSinkBaseAction):

    def __init__(self, databag_lookup : DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def get_file_extension(self) -> str:
        return 'csv'

    def sink(self, parameters: dict, databag: DataBag, file_path: str):
        self.logger.debug('executing : CSVSinkAction.sink()')
        csv_headers = databag.data[0].keys()
        with open(file=file_path, mode='w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_headers, delimiter=parameters.get('delimiter', ','))
            writer.writeheader()
            writer.writerows(databag.data)

        self.logger.debug('executing : CSVSinkAction.sink()')
