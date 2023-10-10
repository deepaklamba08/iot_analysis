import os
import sys

sys.path.append(os.environ['PATH_TO_ANALYSIS_APP'])

from src.processor import Orchestrator
from src.models import RuntimeContext
from src.store import ApplicationStore, ExecutionStoreProvider, JobStore
from src.utils import get_logger


class AnalysisApp:

    def __init__(self, app_args: dict):
        self.app_args = app_args
        self.logger = get_logger()

    def create_runtime_context(self) -> RuntimeContext:
        yaml_config = self.__read_config_file()
        app_config = yaml_config['app']
        for key in self.app_args.keys():
            app_config[key] = self.app_args[key]
        return RuntimeContext(app_config)

    def __read_config_file(self):
        import yaml
        with open(self.app_args['config_file'], 'r') as stream:
            try:
                yaml_config = yaml.safe_load(stream)
                return yaml_config
            except yaml.YAMLError as exc:
                raise Exception('error occurred while reading config file', exc)

    def run(self):
        self.logger.info('starting application ...')
        runtime_context = self.create_runtime_context()
        application_store = ApplicationStore(runtime_context.config_file(), self.app_args)
        job_store = JobStore(runtime_context.config_file())
        execution_store = ExecutionStoreProvider.create_execution_store(runtime_context.parameters)
        Orchestrator(application_store=application_store,
                     execution_store=execution_store,
                     job_store=job_store).run_application(context=runtime_context)
        self.logger.info('exiting application ...')


if __name__ == '__main__':
    arguments = sys.argv
    arguments = arguments[1:]
    app_arguments = {arguments[i]: arguments[i + 1] for i in range(0, len(arguments), 2)}
    AnalysisApp(app_arguments).run()
