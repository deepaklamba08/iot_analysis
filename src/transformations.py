from abc import abstractmethod

from src.models import DataBag, TransformationTemplate
from src.utils import get_logger


def select_databag(parameters: dict) -> DataBag:
    source_type = parameters.get('source_type')
    source_name = parameters.get('source_name')

    if source_type == 'source':
        return parameters['sources_data'][source_name]
    elif source_type == 'transformation':
        return parameters['previous_data'][source_name]
    else:
        raise Exception(f'invalid source_type - {source_type}')


class DummyTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'DummyTransformation'

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : DummyTransformation.execute()')
        return DataBag(name='dummy_databag', provider=self.name(), data=kwargs['data'])


class BaseFieldTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    @abstractmethod
    def select_or_reject(self, item: dict, fields: list) -> dict:
        pass

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : BaseFieldTransformation.execute()')
        databag = select_databag(kwargs)

        fields = kwargs['fields']
        output = list(map(lambda item: self.select_or_reject(item, fields), databag.data))

        self.logger.debug('exiting : BaseFieldTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output)


class FieldSelectorTransformation(BaseFieldTransformation):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'FieldSelectorTransformation'

    def select_or_reject(self, item: dict, fields: list) -> dict:
        op = {}
        for field in fields:
            op[field] = item.get(field, None)
        return op


class FieldRejectTransformation(BaseFieldTransformation):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'FieldRejectTransformation'

    def select_or_reject(self, item: dict, fields: list) -> dict:
        op = {}
        for key in item.keys():
            if key not in fields:
                op[key] = item[key]
        return op


class AddConstantFieldTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'AddConstantFieldTransformation'

    @staticmethod
    def add_field(item: dict, fields_to_add: list) -> dict:
        for field in fields_to_add:
            for key in field:
                item[key] = field[key]
        return item

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : AddConstantFieldTransformation.execute()')
        databag = select_databag(kwargs)

        fields = kwargs['fields']
        output = list(map(lambda item: AddConstantFieldTransformation.add_field(item, fields), databag.data))

        self.logger.debug('exiting : AddConstantFieldTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output)


class RenameFieldTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'RenameFieldTransformation'

    @staticmethod
    def rename_field(item: dict, fields_to_rename: dict) -> dict:
        op = {}
        for field in item.keys():
            if field in fields_to_rename.keys():
                op[fields_to_rename[field]] = item[field]
            else:
                op[field] = item[field]
        return op

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : RenameFieldTransformation.execute()')
        databag = select_databag(kwargs)

        fields = kwargs['fields']
        output = list(map(lambda item: RenameFieldTransformation.rename_field(item, fields), databag.data))

        self.logger.debug('exiting : RenameFieldTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output)


class ConcatFieldTransformation(TransformationTemplate):

    def __init__(self):
        self.logger = get_logger()

    def name(self) -> str:
        return 'ConcatFieldTransformation'

    @staticmethod
    def concat_field(item: dict, source_fields: list, output_field_name: str, seperator: str) -> dict:
        source_values = list(map(lambda field: item.get(field), source_fields))
        if None in source_values:
            item[output_field_name] = None
        else:
            item[output_field_name] = seperator.join(source_values)
        return item

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : ConcatFieldTransformation.execute()')
        databag = select_databag(kwargs)

        fields = kwargs['fields']
        output_field_name = kwargs['output_field']
        seperator = kwargs.get('seperator', '~')
        output = list(
            map(lambda item: ConcatFieldTransformation.concat_field(item, fields, output_field_name, seperator),
                databag.data))

        self.logger.debug('exiting : ConcatFieldTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output)
