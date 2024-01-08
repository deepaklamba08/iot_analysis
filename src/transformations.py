from abc import abstractmethod

from src.models import DataBag, TransformationTemplate, DatabagLookup
from src.utils import get_logger
import json


def select_databag(parameters: dict, databag_lookup: DatabagLookup) -> DataBag:
    source_type = parameters.get('source_type')
    source_name = parameters.get('source_name')

    if source_type == 'source':
        return databag_lookup.get_databag(name=source_name, is_source=True)
    elif source_type == 'transformation':
        return databag_lookup.get_databag(name=source_name, is_source=False)
    else:
        raise Exception(f'invalid source_type - {source_type}')


class DummyTransformation(TransformationTemplate):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'DummyTransformation'

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : DummyTransformation.execute()')
        data = kwargs['data']
        return DataBag(name='dummy_databag', provider=self.name(), data=data, metadata={'row_count': len(data)})


class BaseRecordTransformation(TransformationTemplate):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    @abstractmethod
    def apply(self, item: dict, **kwargs) -> dict:
        pass

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : BaseFieldTransformation.execute()')
        databag = select_databag(kwargs, self.databag_lookup)

        output = list(map(lambda item: self.apply(item, **kwargs), databag.data))

        self.logger.debug('exiting : BaseFieldTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output,
                       metadata={'row_count': len(output)})


class FieldSelectorTransformation(BaseRecordTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'FieldSelectorTransformation'

    def apply(self, item: dict, **kwargs) -> dict:
        fields = kwargs['fields']
        op = {}
        for field in fields:
            op[field] = item.get(field, None)
        return op


class FieldRejectTransformation(BaseRecordTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'FieldRejectTransformation'

    def apply(self, item: dict, **kwargs) -> dict:
        fields = kwargs['fields']
        op = {}
        for key in item.keys():
            if key not in fields:
                op[key] = item[key]
        return op


class AddConstantFieldTransformation(BaseRecordTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'AddConstantFieldTransformation'

    def apply(self, item: dict, **kwargs) -> dict:
        fields_to_add = kwargs['fields']
        for field in fields_to_add:
           item[field] = fields_to_add[field]
        return item


class RenameFieldTransformation(BaseRecordTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'RenameFieldTransformation'

    def apply(self, item: dict, **kwargs) -> dict:
        fields_to_rename = kwargs['fields']
        op = {}
        for field in item.keys():
            if field in fields_to_rename.keys():
                op[fields_to_rename[field]] = item[field]
            else:
                op[field] = item[field]
        return op


class ConcatFieldTransformation(BaseRecordTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'ConcatFieldTransformation'

    def apply(self, item: dict, **kwargs) -> dict:
        source_fields = kwargs['fields']
        output_field_name = kwargs['output_field']
        seperator = kwargs.get('seperator', '~')
        source_values = list(map(lambda field: item.get(field), source_fields))
        if None in source_values:
            item[output_field_name] = None
        else:
            item[output_field_name] = seperator.join(source_values)
        return item


class RecordToJsonTransformation(BaseRecordTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'RecordToJsonTransformation'

    def apply(self, item: dict, **kwargs) -> dict:
        return {'root': json.dumps(item)}


class BaseAttributeTransformation(TransformationTemplate):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    @abstractmethod
    def process_attribute(self, attribute_value):
        pass

    def __process_record(self, record: dict, attribute) -> dict:
        data = []
        for field in record.keys():
            if field == attribute:
                data[field] = self.process_attribute(record[attribute])
            else:
                data[field] = record[field]

    def execute(self, **kwargs) -> DataBag:
        self.logger.debug('executing : BaseAttributeTransformation.execute()')
        databag = select_databag(kwargs, self.databag_lookup)

        attribute = kwargs['attribute']
        output = list(map(lambda item: self.__process_record(item, attribute), databag.data))

        self.logger.debug('exiting : BaseAttributeTransformation.execute()')
        return DataBag(name=f'{self.name()}_databag', provider=self.name(), data=output,
                       metadata={'row_count': len(output)})


class AttributeToJsonTransformation(BaseAttributeTransformation):

    def __init__(self, databag_lookup: DatabagLookup):
        self.logger = get_logger()
        self.databag_lookup = databag_lookup

    def name(self) -> str:
        return 'AttributeToJsonTransformation'

    def process_attribute(self, attribute_value):
        pass
