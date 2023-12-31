import copy
import datetime
from abc import ABC, abstractmethod

from src.models import RuntimeContext, Application, Source, Transformation, Action, SourceTemplate, \
    TransformationTemplate, ActionTemplate, DatabagRegistry, Job
from src.store import ApplicationStore, ExecutionStore, JobStore
from src.utils import get_logger
from src.utils import load_module, Constants


class ProcessResult:

    def __init__(self, status: bool, message: str, inference: dict = {}):
        self.status = status
        self.message = message
        self.inference = inference


class Processor(ABC):

    @abstractmethod
    def process(self) -> ProcessResult:
        pass

    def run(self) -> ProcessResult:
        try:
            return self.process()
        except Exception as ex:
            return ProcessResult(False, f'error occurred while executing processor, cause - {ex}')


class SourceProcessor(Processor):

    def __init__(self, sources: list, runtime_context: RuntimeContext, databag_registry: DatabagRegistry):
        self.sources = sources
        self.source_providers = {'click_house': 'src.sources.ClickHouseSource',
                                 'json': 'src.sources.JsonSource',
                                 'csv': 'src.sources.CsvSource',
                                 'mongo_db': 'src.sources.MongoDbSource',
                                 'dev_source': 'src.sources.DevDataSource',
                                 'db_source': 'src.sources.DbSource'
                                 }
        self.logger = get_logger()
        self.runtime_context = runtime_context
        self.databag_registry = databag_registry

    def __process_source(self, source: Source):
        self.logger.debug(f'processing source - {source.name}')
        provider = self.source_providers.get(source.source_type)
        if not provider and source.source_type == 'custom':
            provider = source.config['provider']

        if not provider:
            raise Exception(f'source type not supported- {source.source_type}')

        source_provider = load_module(module_name=provider)
        if isinstance(source_provider, SourceTemplate):
            parameters = copy.copy(source.config)
            parameters['runtime_context'] = self.runtime_context
            return source_provider.load(**parameters)
        else:
            raise Exception(f'invalid provider - {provider}, expected a provider of type SourceTemplate')

    def process(self) -> ProcessResult:
        self.logger.debug('executing : SourceProcessor.process()')
        for source in self.sources:
            if source.status:
                result = self.__process_source(source)
                self.databag_registry.source_databag(name=source.name, databag=result)
            else:
                self.logger.debug(f'skipping source {source} ...')

        self.logger.debug('executing : SourceProcessor.process()')
        return ProcessResult(True, 'success')


class TransformationProcessor(Processor):

    def __init__(self, transformations: list,
                 runtime_context: RuntimeContext,
                 databag_registry: DatabagRegistry):
        self.transformations = transformations
        self.transformation_providers = {'dummy_transformation': 'src.transformations.DummyTransformation',
                                         'message_format_transformation': 'src.extension.MessageFormatterTransformation',
                                         'field_selector': 'src.transformations.FieldSelectorTransformation',
                                         'field_reject': 'src.transformations.FieldRejectTransformation',
                                         'add_field': 'src.transformations.AddConstantFieldTransformation',
                                         'rename_field': 'src.transformations.RenameFieldTransformation',
                                         'concat_field': 'src.transformations.ConcatFieldTransformation',
                                         'record_to_json':'src.transformations.RecordToJsonTransformation'}
        self.logger = get_logger()
        self.runtime_context = runtime_context
        self.databag_registry = databag_registry

    def __process_transformation(self, transformation: Transformation):
        self.logger.debug(f'processing transformation - {transformation.name}')
        provider = self.transformation_providers.get(transformation.transformation_type)
        if not provider and transformation.transformation_type == 'custom':
            provider = transformation.config['provider']

        if not provider:
            self.logger.error(f'transformation type not supported- {transformation.transformation_type}')
            raise Exception(f'transformation type not supported- {transformation.transformation_type}')

        transformation_provider = load_module(module_name=provider, databag_lookup=self.databag_registry.get_lookup())
        if isinstance(transformation_provider, TransformationTemplate):
            tr_config = copy.copy(transformation.config)
            return transformation_provider.execute(**tr_config)
        else:
            raise Exception(f'invalid provider - {provider}, expected a provider of type SourceTemplate')

    def process(self) -> ProcessResult:
        self.logger.debug('executing : TransformationProcessor.process()')
        for transformation in self.transformations:
            if transformation.status:
                result = self.__process_transformation(transformation)
                # data_dict[transformation.name] = result
                self.databag_registry.transformation_databag(name=transformation.name, databag=result)
            else:
                self.logger.debug(f'skipping transformation {transformation} ...')

        self.logger.debug('exiting : TransformationProcessor.process()')
        return ProcessResult(True, 'success')


class ActionProcessor(Processor):

    def __init__(self, actions: list, data_dict: dict, runtime_context: RuntimeContext,
                 databag_registry: DatabagRegistry):
        self.actions = actions
        self.data_dict = data_dict
        self.action_providers = {'log_data': 'src.actions.LogDataAction',
                                 'telegram_message': 'src.extension.TelegramMessageAction',
                                 'email_notification': 'src.extension.EmailNotificationAction',
                                 'json_sink': 'src.actions.JsonSinkAction',
                                 'csv_sink': 'src.actions.CSVSinkAction'}
        self.logger = get_logger()
        self.runtime_context = runtime_context
        self.databag_registry = databag_registry

    def __process_action(self, action: Action):
        self.logger.debug(f'processing action - {action.name}')
        provider = self.action_providers.get(action.action_type)
        if not provider and action.action_type == 'custom':
            provider = action.config['provider']

        if not provider:
            raise Exception(f'action type not supported- {action.action_type}')

        action_provider = load_module(module_name=provider, databag_lookup=self.databag_registry.get_lookup())
        if isinstance(action_provider, ActionTemplate):
            action_config = copy.copy(action.config)
            action_provider.call(**action_config)
        else:
            raise Exception(f'invalid provider - {provider}, expected a provider of type SourceTemplate')

    def process(self) -> ProcessResult:
        self.logger.debug('executing : ActionProcessor.process()')
        data_dict = {}
        for action in self.actions:
            if action.status:
                result = self.__process_action(action)
                data_dict[action.name] = result
            else:
                self.logger.debug(f'skipping action {action} ...')
        self.logger.debug('exiting : ActionProcessor.process()')
        return ProcessResult(True, 'success')


class ApplicationProcessor(Processor):

    def __init__(self, application: Application, runtime_context: RuntimeContext):
        self.application = application
        self.logger = get_logger()
        self.runtime_context = runtime_context
        self.databag_registry = DatabagRegistry()

    def __generate_metrics(self):

        source_metrics = list(map(lambda databag: {
            'type': 'Source',
            'name': databag.name,
            'provider': databag.provider,
            'records': databag.metadata.get('row_count')
        }, self.databag_registry.get_lookup().all_sources_databags().values()))

        transformation_metrics = list(map(lambda databag: {
            'type': 'Transformation',
            'name': databag.name,
            'provider': databag.provider,
            'records': databag.metadata.get('row_count')
        }, self.databag_registry.get_lookup().all_transformation_databags().values()))

        return {'databag_metrics': source_metrics + transformation_metrics}

    def process(self) -> ProcessResult:
        self.logger.debug('executing : ApplicationProcessor.process()')

        if not self.application.status:
            self.logger.error(f'application id - {self.application.application_id()} is disabled')
            return ProcessResult(False, f'application id - {self.application.application_id()} is disabled')

        self.logger.debug('processing sources ...')
        execution_result = SourceProcessor(sources=self.application.sources,
                                           runtime_context=self.runtime_context,
                                           databag_registry=self.databag_registry).run()
        if execution_result.status:
            self.logger.debug('processing transformations ...')
            execution_result = TransformationProcessor(transformations=self.application.transformations,
                                                       runtime_context=self.runtime_context,
                                                       databag_registry=self.databag_registry).run()
            if execution_result.status:
                self.logger.debug('processing actions ...')
                execution_result = ActionProcessor(actions=self.application.actions,
                                                   data_dict={},
                                                   runtime_context=self.runtime_context,
                                                   databag_registry=self.databag_registry).run()
        self.logger.debug('exiting : ApplicationProcessor.process()')
        return ProcessResult(status=execution_result.status, message=execution_result.message,
                             inference=self.__generate_metrics())


class AppExecutionResult:

    def __init__(self, app_id: str, execution_id: str):
        self.app_id = app_id
        self.execution_id = execution_id


class Orchestrator:

    def __init__(self, application_store: ApplicationStore, execution_store: ExecutionStore, job_store: JobStore):
        self.application_store = application_store
        self.execution_store = execution_store
        self.job_store = job_store
        self.logger = get_logger()

    def schedule_job(self, job: Job, submitter: str = '-', run_type: str = '-', parameters: dict = {}) -> str:
        self.logger.debug('executing : Orchestrator.schedule()')

        application = self.application_store.lookup_application(job.application_id)
        if not application:
            self.logger.error(f'application not found by id - {job.application_id}')
            raise Exception(f'application not found by id - {job.application_id}')

        execution_id = self.execution_store.create_summary(job_id=job.object_id,
                                                           app_id=job.application_id,
                                                           status='scheduled',
                                                           message='app is scheduled',
                                                           run_by=submitter,
                                                           run_type=run_type,
                                                           parameters=parameters)

        self.logger.debug('exiting : Orchestrator.schedule()')
        return execution_id

    def run_scheduled_application(self, execution_id: str,
                        application: Application,
                        context: RuntimeContext) -> AppExecutionResult:
        self.logger.debug(f'executing : Orchestrator.run_application()')

        self.__run_job(execution_id=execution_id,
                       application=application,
                       runtime_context=context)
        self.logger.debug(f'exiting : Orchestrator.run_application()')

    def run_application(self, context: RuntimeContext) -> AppExecutionResult:
        self.logger.debug('executing : Orchestrator.orchestrate()')

        application = self.application_store.lookup_application(context.app_id())
        if not application:
            self.logger.error(f'application not found by id - {context.app_id()}')
            raise Exception(f'application not found by id - {context.app_id()}')

        execution_id = self.execution_store.create_summary(job_id=context.get_value("job_id", context.app_id()),
                                                           app_id=context.app_id(),
                                                           status='executing',
                                                           message='app is running',
                                                           run_by=context.get_value('submitter', '-'),
                                                           run_type=context.get_value('run_type', '-'),
                                                           parameters=context.parameters)

        self.__run_job(execution_id=execution_id, application=application, runtime_context=context)
        exe_result = self.logger.debug('exiting : Orchestrator.orchestrate()')
        return exe_result

    def __run_job(self, execution_id: str, application: Application, runtime_context: RuntimeContext):
        process_result = ApplicationProcessor(application=application, runtime_context=runtime_context).run()
        if not process_result.status:
            self.execution_store.update_summary(execution_id=execution_id,
                                                **{'status': 'Failed',
                                                   'message': f'Execution failed with error - {process_result.message}',
                                                   'end_time': datetime.datetime.now().strftime(Constants.DATE_FORMAT)
                                                   })
            self.logger.error(f'Execution failed with error - {process_result.message}')
            raise Exception(f'Execution failed with error - {process_result.message}')

        self.execution_store.update_summary(execution_id=execution_id, **{'status': 'Completed',
                                                                          'message': 'App execution completed',
                                                                          'metrics': process_result.inference,
                                                                          'end_time': datetime.datetime.now().strftime(
                                                                              Constants.DATE_FORMAT)
                                                                          })
        return AppExecutionResult(app_id=application.object_id, execution_id=execution_id)
