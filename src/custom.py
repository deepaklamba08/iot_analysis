from src.models import DataBag, SourceTemplate, TransformationTemplate, ActionTemplate
import clickhouse_connect
import json
from src.utils import get_logger, get_credentials
import csv


class ClickHouseSource(SourceTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'ClickHouseSource'

    def load(self, **kwargs) -> DataBag:
        self.logger.debug('executing : ClickHouseSource.load()')
        credentials = get_credentials(kwargs['credential_provider'])
        client = clickhouse_connect.get_client(
            host=credentials['host'], port=credentials['port'], username=credentials['user'],
            password=credentials['password'])
        result = client.query(kwargs['query'])
        self.logger.debug('exiting : ClickHouseSource.load()')
        return DataBag(name='clickhouse_databag', provider=self.name(), data=result.result_rows)


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
            return DataBag(name='json_databag', provider=self.name(), data=result)


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
            return DataBag(name='csv_databag', provider=self.name(), data=result)


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
    def __log_data(data):
        if data:
            for key in data:
                databag = data[key]
                print('-----------------------------------------------------------')
                print(databag)
                print(f'metadata - {databag.metadata}')
                print(f'data - {databag.data}')

    def call(self, **kwargs):
        self.logger.debug('executing : LogDataAction.call()')
        data = kwargs.get('data')
        if data:
            LogDataAction.__log_data(data.get('sources_data'))
            LogDataAction.__log_data(data.get('transformation_data'))

        self.logger.debug('exiting : LogDataAction.call()')
