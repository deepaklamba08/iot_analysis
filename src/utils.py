import importlib
import logging
import os
import re
from abc import ABC, abstractmethod


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
    from pyaml_env import parse_config

    if not os.path.isfile(config_file_path):
        raise Exception(f'not a file - {config_file_path}')

    if not os.path.exists(config_file_path):
        raise Exception(f'file not exists - {config_file_path}')

    yaml_config = parse_config(config_file_path)
    return yaml_config


def get_env_config(key: str):
    env_config = os.environ.get(key)
    if not env_config:
        raise Exception(f'key not found in environment - {key}')
    return env_config


class PlatformFunctions(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def get_matcher(self):
        pass

    @abstractmethod
    def get_functions(self):
        pass

    @staticmethod
    def __split_args(arg_str):
        args = []
        current = ''
        depth = 0
        for char in arg_str:
            if char == ',' and depth == 0:
                args.append(current)
                current = ''
            else:
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                current += char
        if current:
            args.append(current)
        return args

    @staticmethod
    def __evaluate_function_call(call_str: str, functions: str, context: dict):
        # Extract function name
        func_name_match = re.match(r'(\w+)\((.*)\)', call_str)
        if not func_name_match:
            return f"<invalid:{call_str}>"

        func_name, args_str = func_name_match.groups()
        if func_name not in functions:
            return f"<unknown:{func_name}>"

        # Parse arguments recursively (simplified but works for our use case)
        args = PlatformFunctions.__split_args(args_str)
        resolved_args = [
            PlatformFunctions.__evaluate_function_call(arg, functions, context) if re.match(r'\w+\(.*\)',
                                                                                            arg.strip()) else eval(
                arg.strip()) for arg in args]

        try:
            resolved_args.insert(0, context)
            return functions[func_name](*resolved_args)
        except Exception as e:
            return f"<error:{e}>"

    def extract_and_replace_conf_functions(self, text: str, context: dict):
        pattern = re.compile(self.get_matcher())
        i = 0
        output = ''

        functions = self.get_functions()
        while i < len(text):
            match = pattern.search(text, i)
            if not match:
                output += text[i:]
                break

            # Append text before match
            output += text[i:match.start()]

            # Extract full function call (with nested support)
            start_index = match.start(1)
            func_start = match.group(1)
            depth = 0
            j = start_index
            while j < len(text):
                if text[j] == '(':
                    depth += 1
                elif text[j] == ')':
                    depth -= 1
                    if depth == 0:
                        break
                j += 1

            full_call = text[start_index:j + 1]  # e.g., get_value(get_env('dir'), 'fallback')
            evaluated = str(PlatformFunctions.__evaluate_function_call(full_call, functions, context))
            output += evaluated
            i = j + 1

        return output


class ConfigFunctions(PlatformFunctions):

    def __init__(self):
        pass

    def get_matcher(self):
        return r'conf:(\w+\()'

    @staticmethod
    def __get_env(context: dict, key: str):
        return get_env_config(key)

    @staticmethod
    def __get_value(context: dict, key: str, fallback: str = 'default'):
        return context.get(key, fallback)

    def get_functions(self):
        functions = {
            'get_env': ConfigFunctions.__get_env,
            'get_value': ConfigFunctions.__get_value
        }
        return functions


class RecordFunctions(PlatformFunctions):

    def __init__(self):
        pass

    def get_matcher(self):
        return r'record:(\w+\()'

    @staticmethod
    def __get_value(context: dict, key: str, fallback: str = 'default'):
        return context.get(key, fallback)

    def get_functions(self):
        functions = {
            'get_value': RecordFunctions.__get_value
        }
        return functions


def replace_variables(text: str, conf: bool = True, record: bool = True, context: dict = {}, record_data: dict = {}):
    if conf and record:
        conf_res = ConfigFunctions().extract_and_replace_conf_functions(text=text, context=context)
        return RecordFunctions().extract_and_replace_conf_functions(text=conf_res, context=record_data)
    elif not conf and record:
        return RecordFunctions().extract_and_replace_conf_functions(text=text, context=record_data)
    elif conf and not record:
        return ConfigFunctions().extract_and_replace_conf_functions(text=text, context=context)
    else:
        raise Exception('Invalid combination of conf and record flags')