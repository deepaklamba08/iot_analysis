import copy
from abc import ABC, abstractmethod

from src.models import RuntimeContext, Application, Source, Transformation, Action, SourceTemplate, \
    TransformationTemplate, ActionTemplate
from src.store import ApplicationStore
from src.utils import get_logger
from src.utils import load_module


class ProcessResult:

    def __init__(self, status: bool, message: str, data: dict):
        self.status = status
        self.message = message
        self.data = data


class Processor(ABC):

    @abstractmethod
    def process(self) -> ProcessResult:
        pass

    def run(self) -> ProcessResult:
        try:
            return self.process()
        except Exception as ex:
            return ProcessResult(False, f'error occurred while executing processor, cause - {ex}', None)


class SourceProcessor(Processor):

    def __init__(self, sources: list):
        self.sources = sources
        self.source_providers = {'click_house': 'src.custom.ClickHouseSource',
                                 'json': 'src.custom.JsonSource',
                                 'csv': 'src.custom.CsvSource'}
        self.logger = get_logger()

    def __process_source(self, source: Source):
        self.logger.debug(f'processing source - {source.name}')
        provider = self.source_providers.get(source.source_type)
        if not provider and source.source_type == 'custom':
            provider = source.config['provider']

        if not provider:
            raise Exception(f'source type not supported- {source.source_type}')

        source_provider = load_module(provider)
        if isinstance(source_provider, SourceTemplate):
            parameters = copy.copy(source.config)
            return source_provider.load(**parameters)
        else:
            raise Exception(f'invalid provider - {provider}, expected a provider of type SourceTemplate')

    def process(self) -> ProcessResult:
        self.logger.debug('executing : SourceProcessor.process()')
        data_dict = {}
        for source in self.sources:
            result = self.__process_source(source)
            data_dict[source.name] = result

        self.logger.debug('executing : SourceProcessor.process()')
        return ProcessResult(True, 'success', data_dict)


class TransformationProcessor(Processor):

    def __init__(self, transformations: list, sources_data: dict):
        self.transformations = transformations
        self.sources_data = sources_data
        self.transformation_providers = {'dummy_transformation': 'src.custom.DummyTransformation'}
        self.logger = get_logger()

    def __process_transformation(self, transformation: Transformation, previous_transformation_data: dict):
        self.logger.debug(f'processing transformation - {transformation.name}')
        provider = self.transformation_providers.get(transformation.transformation_type)
        if not provider and transformation.transformation_type == 'custom':
            provider = transformation.config['provider']

        if not provider:
            self.logger.error(f'transformation type not supported- {transformation.transformation_type}')
            raise Exception(f'transformation type not supported- {transformation.transformation_type}')

        transformation_provider = load_module(provider)
        if isinstance(transformation_provider, TransformationTemplate):
            tr_config = copy.copy(transformation.config)
            tr_config['sources_data'] = self.sources_data
            tr_config['previous_data'] = previous_transformation_data

            return transformation_provider.execute(**tr_config)
        else:
            raise Exception(f'invalid provider - {provider}, expected a provider of type SourceTemplate')

    def process(self) -> ProcessResult:
        self.logger.debug('executing : TransformationProcessor.process()')
        data_dict = {}
        for transformation in self.transformations:
            result = self.__process_transformation(transformation, data_dict)
            data_dict[transformation.name] = result

        self.logger.debug('exiting : TransformationProcessor.process()')
        return ProcessResult(True, 'success', data_dict)


class ActionProcessor(Processor):

    def __init__(self, actions: list, data_dict: dict):
        self.actions = actions
        self.data_dict = data_dict
        self.action_providers = {'log_data': 'src.custom.LogDataAction',
                                 'telegram_message':'src.extension.TelegramMessageAction',
                                 'email_notification':'src.extension.EmailNotificationAction'}
        self.logger = get_logger()

    def __process_action(self, action: Action):
        self.logger.debug(f'processing action - {action.name}')
        provider = self.action_providers.get(action.action_type)
        if not provider and action.action_type == 'custom':
            provider = action.config['provider']

        if not provider:
            raise Exception(f'action type not supported- {action.action_type}')

        action_provider = load_module(provider)
        if isinstance(action_provider, ActionTemplate):
            action_config = copy.copy(action.config)
            action_config['data'] = self.data_dict
            action_provider.call(**action_config)
        else:
            raise Exception(f'invalid provider - {provider}, expected a provider of type SourceTemplate')

    def process(self) -> ProcessResult:
        self.logger.debug('executing : ActionProcessor.process()')
        data_dict = {}
        for action in self.actions:
            result = self.__process_action(action)
            data_dict[action.name] = result

        self.logger.debug('exiting : ActionProcessor.process()')
        return ProcessResult(True, 'success', data_dict)


class ApplicationProcessor(Processor):

    def __init__(self, application: Application):
        self.application = application
        self.logger = get_logger()

    def process(self) -> ProcessResult:
        self.logger.debug('executing : ApplicationProcessor.process()')
        self.logger.debug('processing sources ...')
        execution_result = SourceProcessor(self.application.sources).run()
        data_dict = {'sources_data': execution_result.data}
        if execution_result.status:
            self.logger.debug('processing transformations ...')
            execution_result = TransformationProcessor(self.application.transformations, execution_result.data).run()
            data_dict['transformation_data'] = execution_result.data
            if execution_result.status:
                self.logger.debug('processing actions ...')
                execution_result = ActionProcessor(self.application.actions, data_dict).run()
        self.logger.debug('exiting : ApplicationProcessor.process()')
        return execution_result


class Orchestrator:

    def __init__(self, application_store: ApplicationStore):
        self.application_store = application_store
        self.logger = get_logger()

    def orchestrate(self, context: RuntimeContext):
        self.logger.debug('executing : Orchestrator.orchestrate()')
        application = self.application_store.lookup_application(context.application_id())
        if not application:
            self.logger.error(f'application not found by id - {context.application_id()}')
            raise Exception(f'application not found by id - {context.application_id()}')

        process_result = ApplicationProcessor(application).run()
        if not process_result.status:
            self.logger.error(f'execution failed with error - {process_result.message}')
            raise Exception(f'execution failed with error - {process_result.message}')
        self.logger.debug('exiting : Orchestrator.orchestrate()')
