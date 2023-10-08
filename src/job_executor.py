from src.processor import Orchestrator
from src.models import RuntimeContext
from src.store import ApplicationStore, ExecutionStore
from src.utils import get_logger


class JobExecutor:

    def __init__(self):
        self.logger = get_logger()

    def execute_job(self, parameters: dict):
        self.logger.debug('executing : JobExecutor.execute_job()')

        runtime_context = RuntimeContext(parameters)

        application_store = ApplicationStore(runtime_context.config_file(), parameters)
        execution_store = ExecutionStore({'base_dir': runtime_context.execution_summary_dir()})

        Orchestrator(application_store=application_store,
                     execution_store=execution_store).orchestrate(context=runtime_context)

        self.logger.debug('exiting : JobExecutor.execute_job()')
