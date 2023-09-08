import os
import sys

sys.path.append(os.environ['PATH_TO_APP'])

from src.processor import Orchestrator
from src.models import RuntimeContext
from src.store import ApplicationStore
from src.utils import get_logger


class AnalysisApp:

    def __init__(self, app_args: dict):
        self.app_args = app_args
        self.logger = get_logger()

    def create_runtime_context(self) -> RuntimeContext:
        return RuntimeContext(self.app_args)

    def run(self):
        self.logger.info('starting application ...')
        runtime_context = self.create_runtime_context()
        application_store = ApplicationStore(runtime_context.config_file(), self.app_args)
        Orchestrator(application_store).orchestrate(runtime_context)
        self.logger.info('exiting application ...')


if __name__ == '__main__':
    arguments = sys.argv
    arguments = arguments[1:]
    app_arguments = {arguments[i]: arguments[i + 1] for i in range(0, len(arguments), 2)}
    AnalysisApp(app_arguments).run()
