import logging
import importlib
from abc import ABC, abstractmethod
import os


class Constants:
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class CredentialProvider(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def lookup(self, config: dict) -> dict:
        pass


class SimpleCredentialProvider(CredentialProvider):

    def lookup(self, config: dict) -> dict:
        return config


class EnvironmentVariableCredentialProvider(CredentialProvider):

    def lookup(self, config: dict) -> dict:
        variables = config.get('variables')
        if not variables:
            raise Exception('no variables provided for lookup')
        env_variables = {}
        os_env_variables = os.environ.items()
        for variable_name in variables.keys():
            variable_value = list(filter(lambda pair: pair[0] == variables[variable_name], os_env_variables))
            if len(variable_value) > 0:
                env_variables[variable_name] = variable_value[0][1]
        return env_variables


def get_credentials(config: dict) -> dict:
    provider_type = config['type']
    if provider_type == 'simple':
        return SimpleCredentialProvider().lookup(config)
    elif provider_type == 'environment_variable':
        return EnvironmentVariableCredentialProvider().lookup(config)
    else:
        raise Exception(f'credential provider not supported - {provider_type}')


def get_logger(log_file_name='../logs/app.log'):
    logging.basicConfig(filename=log_file_name,
                        format='[%(asctime)s] [%(name)s] [%(levelname)s] [%(funcName)s:%(lineno)d] %(message)s',
                        filemode='a')
    logger = logging.getLogger()

    logger.setLevel(logging.DEBUG)
    return logger


def load_module(module_name: str, **kwargs):
    module_name, class_name = module_name.rsplit(".", 1)
    module_class = getattr(importlib.import_module(module_name), class_name)
    if kwargs:
        return module_class(**kwargs)
    else:
        return module_class()


def replace_placeholders(raw_data: str, parameters: dict) -> str:
    for key, value in parameters.items():
        raw_data = raw_data.replace('${' + key + '}', f'{value}')
    return raw_data


def read_config_file(config_file_path: str):
    import yaml

    if not os.path.isfile(config_file_path):
        raise Exception(f'not a file - {config_file_path}')

    if not os.path.exists(config_file_path):
        raise Exception(f'file not exists - {config_file_path}')

    with open(config_file_path, 'r') as stream:
        try:
            yaml_config = yaml.safe_load(stream)
            return yaml_config
        except yaml.YAMLError as exc:
            raise Exception('error occurred while reading config file', exc)
