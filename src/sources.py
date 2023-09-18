import csv
import json

import clickhouse_connect
import pymongo

from src.models import DataBag, SourceTemplate
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
