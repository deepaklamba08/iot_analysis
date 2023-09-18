import csv
import json
import os
from abc import abstractmethod
from datetime import datetime
import clickhouse_connect
import pymongo

from src.models import DataBag, SourceTemplate, TransformationTemplate, ActionTemplate
from src.models import RuntimeContext
from src.utils import get_logger, get_credentials, replace_placeholders


class ClickHouseSource(SourceTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'ClickHouseSource'

    @staticmethod
    def __map_row(result_row, column_names: list) -> dict:
        data = {}
        for index, value in enumerate(column_names):
            data[value] = result_row[index]
        return data

    @staticmethod
    def __get_query(query_source: str, value: str, runtime_context: RuntimeContext) -> str:
        if query_source == 'sql':
            return value
        elif query_source == 'file':
            with open(value, 'r') as stream:
                query_str = '\n'.join(stream.readlines())
                return replace_placeholders(raw_data=query_str, parameters=runtime_context.parameters)
        else:
            raise Exception(f'query source - {query_source} not supported')

    def load(self, **kwargs) -> DataBag:
        self.logger.debug('executing : ClickHouseSource.load()')
        credentials = get_credentials(kwargs['credential_provider'])
        client = clickhouse_connect.get_client(
            host=credentials['host'], port=credentials['port'], username=credentials['user'],
            password=credentials['password'])
        result = client.query(
            ClickHouseSource.__get_query(query_source=kwargs.get('query_source', 'sql'),
                                         value=kwargs['query'],
                                         runtime_context=kwargs['runtime_context']))
        column_names = result.column_names
        data_list = list(
            map(lambda result_row: ClickHouseSource.__map_row(result_row, column_names), result.result_rows))
        self.logger.debug('exiting : ClickHouseSource.load()')
        return DataBag(name='clickhouse_databag', provider=self.name(), data=data_list,
                       metadata={'columns': column_names, 'row_count': result.row_count})


class JsonSource(SourceTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'JsonSource'

    def load(self, **kwargs) -> DataBag:
        self.logger.debug('executing : JsonSource.load()')
        file_path = kwargs['file_path']
        with (open(file_path, 'r')) as data_stream:
            result = json.loads('\n'.join(data_stream.readlines()))
            self.logger.debug('exiting : JsonSource.load()')
            return DataBag(name='json_databag', provider=self.name(), data=result,
                           metadata={'file_path': file_path, 'row_count': len(result)})


class CsvSource(SourceTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'CsvSource'

    def load(self, **kwargs) -> DataBag:
        self.logger.debug('executing : CsvSource.load()')
        file_path = kwargs['file_path']
        with (open(file_path, 'r')) as data_stream:
            csv_file = csv.DictReader(data_stream, delimiter=',', quotechar='|')
            result = list(map(lambda line: line, csv_file))
            self.logger.debug('exiting : CsvSource.load()')
            return DataBag(name='csv_databag', provider=self.name(), data=result,
                           metadata={'file_path': file_path, 'row_count': len(result)})


class DummyTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'DummyTransformation'

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : DummyTransformation.execute()')
        return DataBag(name='dummy_databag', provider=self.name(), data=kwargs['data'])


class LogDataAction(ActionTemplate):

    def __init__(self):
        self.logger = get_logger()

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
        data = kwargs.get('data')
        sources_log = kwargs.get('sources_to_log')
        tr_log = kwargs.get('transformation_to_log')

        if sources_log is None and tr_log is None:
            LogDataAction.__log_data(data.get('sources_data'))
            LogDataAction.__log_data(data.get('transformation_data'))
        elif sources_log is not None:
            for sources_name in sources_log:
                source_data = data.get('sources_data').get(sources_name)
                LogDataAction.__log_databag(source_data)
        elif tr_log is not None:
            for target_name in tr_log:
                target_data = data.get('transformation_data').get(target_name)
                LogDataAction.__log_databag(target_data)
        else:
            pass

        self.logger.debug('exiting : LogDataAction.call()')


class DataSinkBaseAction(ActionTemplate):

    def __init__(self):
        self.logger = get_logger()

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
            databag = kwargs['data']['sources_data'][source_name]
        elif source_type == 'transformation':
            databag = kwargs['data']['transformation_data'][source_name]
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

    def __init__(self):
        self.logger = get_logger()

    def get_file_extension(self) -> str:
        return 'json'

    def sink(self, parameters: dict, databag: DataBag, file_path: str):
        self.logger.debug('executing : JsonSinkAction.sink()')
        json_object = json.dumps(databag.data, indent=4)

        with open(file=file_path, mode='w') as outfile:
            outfile.write(json_object)
        self.logger.debug('executing : JsonSinkAction.sink()')


class CSVSinkAction(DataSinkBaseAction):

    def __init__(self):
        self.logger = get_logger()

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


class MongoDbSource(SourceTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'MongoDbSource'

    def load(self, **kwargs) -> DataBag:
        self.logger.debug('executing : MongoDbSource.load()')
        credentials = get_credentials(kwargs['credential_provider'])
        mong_client = pymongo.MongoClient(host=credentials['host'],
                                          port=credentials['port'],
                                          username=credentials['user'],
                                          password=credentials['password'])

        database_name = kwargs['database']
        if database_name not in mong_client.list_database_names():
            self.logger.error(f'database - {database_name} not found')
            raise Exception(f'database - {database_name} not found')

        database = mong_client.get_database(database_name)

        collection_name = kwargs['collection']
        if collection_name not in database.list_collection_names():
            self.logger.error(f'collection - {collection_name} not found')
            raise Exception(f'collection - {collection_name} not found')
        collection = database.get_collection(collection_name)

        projection = kwargs.get('projection', None)
        if projection:
            data_list = collection.find(filter=kwargs.get('filter', {}),
                                        projection=projection,
                                        limit=kwargs.get('limit', 0))
        else:
            data_list = collection.find(filter=kwargs.get('filter', {}),
                                        limit=kwargs.get('limit', 0))

        self.logger.debug('exiting : MongoDbSource.load()')
        return DataBag(name='mongodb_databag', provider=self.name(),
                       data=list(map(lambda item: item, data_list)),
                       metadata={})


class BaseFieldTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    @abstractmethod
    def select_or_reject(self, item: dict, fields: list) -> dict:
        pass

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : BaseFieldTransformation.execute()')
        source_type = kwargs.get('source_type')
        source_name = kwargs.get('source_name')

        if source_type == 'source':
            databag = kwargs['sources_data'][source_name]
        elif source_type == 'transformation':
            databag = kwargs['previous_data'][source_name]
        else:
            raise Exception(f'invalid source_type - {source_type}')

        fields = kwargs['fields']
        output = list(map(lambda item: self.select_or_reject(item, fields), databag.data))

        self.logger.debug('exiting : BaseFieldTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output)


class FieldSelectorTransformation(BaseFieldTransformation):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'FieldSelectorTransformation'

    def select_or_reject(self, item: dict, fields: list) -> dict:
        op = {}
        for field in fields:
            op[field] = item.get(field, None)
        return op


class FieldRejectTransformation(BaseFieldTransformation):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'FieldRejectTransformation'

    def select_or_reject(self, item: dict, fields: list) -> dict:
        op = {}
        for key in item.keys():
            if key not in fields:
                op[key] = item[key]
        return op


class AddConstantFieldTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'AddConstantFieldTransformation'

    @staticmethod
    def add_field(item: dict, fields_to_add: list) -> dict:
        for field in fields_to_add:
            for key in field:
                item[key] = field[key]
        return item

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : AddConstantFieldTransformation.execute()')
        source_type = kwargs.get('source_type')
        source_name = kwargs.get('source_name')

        if source_type == 'source':
            databag = kwargs['sources_data'][source_name]
        elif source_type == 'transformation':
            databag = kwargs['previous_data'][source_name]
        else:
            raise Exception(f'invalid source_type - {source_type}')

        fields = kwargs['fields']
        output = list(map(lambda item: AddConstantFieldTransformation.add_field(item, fields), databag.data))

        self.logger.debug('exiting : AddConstantFieldTransformation.execute()')
        return DataBag(name='dummy_databag', provider=self.name(), data=output)
