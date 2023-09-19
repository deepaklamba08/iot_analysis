import requests

from src.models import DataBag, TransformationTemplate, ActionTemplate,DatabagRegistry
from src.utils import get_logger, get_credentials


class TelegramMessageAction(ActionTemplate):

    def __init__(self, databag_registry: DatabagRegistry):
        self.logger = get_logger()
        self.databag_registry = databag_registry

    def __get_messages(self,parameters: dict):
        message_source_type = parameters.get('message_source_type')
        message_source_name = parameters.get('message_source_name')

        if message_source_type == 'source':
            telegram_data = self.databag_registry.get_databag(name=message_source_name, is_source=True)
        elif message_source_type == 'transformation':
            telegram_data = self.databag_registry.get_databag(name=message_source_name, is_source=False)
        else:
            raise Exception(f'invalid message_source_type - {message_source_type}')

        return list(map(lambda element: element['message'], telegram_data.data))

    def call(self, **kwargs):
        self.logger.debug('executing : TelegramMessageAction.call()')
        credentials = get_credentials(kwargs['credential_provider'])

        api_token = credentials.get('api_token')
        if not api_token:
            raise Exception('api_token is invalid, please set TELEGRAM_API_TOKEN')
        chat_id = credentials.get('chat_id')
        if not chat_id:
            raise Exception('chat_id is invalid, please set TELEGRAM_CHAT_ID')

        try:
            api_url = f'https://api.telegram.org/bot{api_token}/sendMessage'
            messages = self.__get_messages(kwargs)
            for message in messages:
                response = requests.post(api_url,
                                         json={'chat_id': chat_id,
                                               'text': message})
        except Exception as ex:
            raise Exception('error occurred while sending telegram message', ex)

        self.logger.debug('exiting : TelegramMessageAction.call()')


class EmailNotificationAction(ActionTemplate):

    def __init__(self, databag_registry: DatabagRegistry):
        self.logger = get_logger()
        self.databag_registry = databag_registry

    def call(self, **kwargs):
        self.logger.debug('executing : EmailNotificationAction.call()')
        # add handling code
        self.logger.debug('exiting : EmailNotificationAction.call()')


class MessageFormatterTransformation(TransformationTemplate):

    def __init__(self, databag_registry: DatabagRegistry):
        self.logger = get_logger()
        self.databag_registry = databag_registry

    def name(self) -> str:
        return 'MessageFormatterTransformation'

    @staticmethod
    def __format_message(message_fields: dict, data: dict, message_title: str) -> str:
        message = '\n'.join(
            list(map(lambda key: f"{message_fields.get(key)} - {data.get(key)}", message_fields.keys())))
        return {'message': f'{message_title}\n{message}'}

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : MessageFormatterTransformation.execute()')
        parameters = kwargs
        message_source_type = parameters.get('message_source_type')
        message_source_name = parameters.get('message_source_name')

        if message_source_type == 'source':
            message_data = self.databag_registry.get_databag(name=message_source_name, is_source=True)
        elif message_source_type == 'transformation':
            message_data = self.databag_registry.get_databag(name=message_source_name, is_source=False)
        else:
            raise Exception(f'invalid message_source_type - {message_source_type}')
        message_fields = kwargs.get('message_fields')
        formatted_messages = list(
            map(lambda data_point: MessageFormatterTransformation.__format_message(
                message_fields=message_fields, data=data_point,
                message_title=kwargs.get('message_title')), message_data.data))
        return DataBag(name='dummy_databag', provider=self.name(), data=formatted_messages)
