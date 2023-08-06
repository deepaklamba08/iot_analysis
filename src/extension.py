import requests

from src.models import ActionTemplate
from src.utils import get_logger, get_credentials


class TelegramMessageAction(ActionTemplate):

    def __init__(self):
        self.logger = get_logger()

    @staticmethod
    def __get_messages(parameters: dict):
        message_source_type = parameters.get('message_source_type')
        message_source_name = parameters.get('message_source_name')

        if message_source_type == 'source':
            telegram_data = parameters.get('data')['sources_data'][message_source_name]
        elif message_source_type == 'transformation':
            telegram_data = parameters.get('data')['transformation_data'][message_source_name]
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
            messages = TelegramMessageAction.__get_messages(kwargs)
            for message in messages:
                response = requests.post(api_url,
                                         json={'chat_id': chat_id,
                                               'text': message})
        except Exception as ex:
            raise Exception('error occurred while sending telegram message', ex)

        self.logger.debug('exiting : TelegramMessageAction.call()')


class EmailNotificationAction(ActionTemplate):

    def __init__(self):
        self.logger = get_logger()

    def call(self, **kwargs):
        self.logger.debug('executing : EmailNotificationAction.call()')
        # add handling code
        self.logger.debug('exiting : EmailNotificationAction.call()')
