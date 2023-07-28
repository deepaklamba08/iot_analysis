import logging
import importlib
from abc import ABC, abstractmethod
import os


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
        variable_names = config['variable_names']
        env_variables = {}
        for key, value in os.environ.items():
            if key in variable_names:
                env_variables[key] = value
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
                        format='%(asctime)s %(message)s',
                        filemode='a')
    logger = logging.getLogger()

    logger.setLevel(logging.DEBUG)
    return logger


def load_module(module_name: str):
    module_name, class_name = module_name.rsplit(".", 1)
    module_class = getattr(importlib.import_module(module_name), class_name)
    return module_class()
