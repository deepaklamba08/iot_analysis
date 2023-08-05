from src.models import Application, Source, Transformation, Action
import json
from src.utils import get_logger


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

    def __replace_placeholders(self, raw_data):
        for key, value in self.parameters.items():
            raw_data = raw_data.replace('${' + key + '}', f'{value}')
        return raw_data

    def __load_applications(self):
        if not self.config_file:
            raise Exception(f'config file is invalid - {self.config_file}')
        self.applications: dict = {}
        with open(self.config_file, 'r') as data_stream:
            config_str = self.__replace_placeholders('\n'.join(data_stream.readlines()))
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
