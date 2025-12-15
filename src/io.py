from datetime import datetime
import json
import os
from abc import ABC, abstractmethod
from typing import List, Any, Callable, Dict


class FileMetadata:

    def __init__(self, file_name: str,
                 file_path: str,
                 record_count: int,
                 create_date: datetime,
                 created_by: str,
                 external_file: bool = False):
        self.file_name = file_name
        self.file_path = file_path
        self.record_count = record_count
        self.create_date = create_date
        self.created_by = created_by
        self.external_file = external_file


class ObjectMetadata:
    def __init__(self, object_name: str,
                 create_date: datetime,
                 file_count: int,
                 created_by: str,
                 file_format: str,
                 location: str,
                 file_metadata: list[FileMetadata] = [],
                 update_date: datetime = None,
                 properties: dict = {}):
        self.object_name = object_name
        self.create_date = create_date
        self.file_count = file_count
        self.created_by = created_by
        self.file_format = file_format
        self.location = location
        self.file_metadata = file_metadata
        self.update_date = update_date
        self.properties = properties


class JsonFileUtil:

    @staticmethod
    def read_file(file_path: str):
        with open(file_path, 'r') as data_stream:
            return json.loads('\n'.join(data_stream.readlines()))

    @staticmethod
    def write_file(file_path: str, data: list, mode='w', encoding='utf-8'):
        with open(file=file_path, mode=mode, encoding=encoding) as stream:
            stream.write(json.dumps(data, indent=0))


class ObjectStore:

    def __init__(self, store_path):
        base_dir = os.path.join(store_path, 'db')
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        self.store_path = os.path.join(base_dir, 'store.json')

        if not os.path.isfile(self.store_path):
            JsonFileUtil.write_file(self.store_path, [], 'x', 'utf-8')

    def register_object(self, object_name: str, file_format: str, location: str, properties: dict = {}):
        existing = self.get_object_metadata(object_name=object_name)
        if existing:
            raise ValueError(f'object already exists- {object_name}')

        if file_format != 'json':
            raise ValueError(f'file format not supported- {file_format}')

        if not location:
            raise ValueError(f'location must be provided')

        obj_loc = os.path.join(location, object_name)
        if not os.path.exists(obj_loc):
            os.makedirs(obj_loc)
        else:
            raise ValueError(f'location already exist- {obj_loc}')

        metadata = ObjectMetadata(
            object_name=object_name,
            create_date=datetime.now(),
            file_count=0,
            created_by='',
            file_format=file_format,
            location=obj_loc,
            properties=properties
        )
        self.__update_object(metadata)

    def list_objects(self):
        return self.__read_all()

    def get_object_metadata(self, object_name: str) -> ObjectMetadata:
        existing = list(filter(lambda element: element.object_name == object_name, self.__read_all()))
        return existing[0] if len(existing) > 0 else None

    def add_file(self, object_name: str, file_metadata: FileMetadata):
        existing = self.get_object_metadata(object_name=object_name)
        if not existing:
            raise ValueError(f'object not exists- {object_name}')
        existing.file_metadata.append(file_metadata)
        self.__update_object(existing)

    def query_object(self, object_name: str):
        return QueryStatement(object_name=object_name, object_store=self)

    def get_object_metadata(self, object_name):
        elements = self.__read_all()
        elements = list(filter(lambda element: element.object_name == object_name, elements))
        return elements[0] if len(elements) > 0 else None

    def __read_all(self) -> list:
        elements = JsonFileUtil.read_file(self.store_path)
        return list(map(lambda element: ObjectStore.__parse_object_metadata(element), elements))

    def __update_object(self, metadata: ObjectMetadata):
        elements = self.__read_all()
        remaining = list(filter(lambda element: element.object_name != metadata.object_name, elements))
        remaining.append(metadata)
        self.__save_objects(remaining)

    def __save_objects(self, metadata_list: list):
        metadata_list = sorted(metadata_list, key=lambda element: element.create_date)
        data = list(map(lambda element: ObjectStore.__object_metadata_to_dict(element), metadata_list))
        JsonFileUtil.write_file(self.store_path, data, 'w')

    @staticmethod
    def __parse_file_metadata(data: dict) -> FileMetadata:
        return FileMetadata(
            file_name=data["file_name"],
            file_path=data["file_path"],
            record_count=int(data["record_count"]),
            create_date=datetime.fromisoformat(data["create_date"]),
            created_by=data["created_by"],
            external_file=data.get("external_file", False)
        )

    @staticmethod
    def __parse_object_metadata(data: dict) -> ObjectMetadata:
        fm_list = list(map(lambda fm: ObjectStore.__parse_file_metadata(fm), data.get('file_metadata'))) if data.get(
            'file_metadata') else []
        return ObjectMetadata(
            object_name=data["object_name"],
            create_date=datetime.fromisoformat(data["create_date"]),
            file_count=int(data["file_count"]),
            created_by=data["created_by"],
            file_format=data["file_format"],
            location=data['location'],
            file_metadata=fm_list,
            update_date=datetime.fromisoformat(data["update_date"]) if data.get("update_date") else None,
            properties=data.get("properties", {})
        )

    @staticmethod
    def __file_metadata_to_dict(file_metadata: FileMetadata) -> dict:
        return {
            "file_name": file_metadata.file_name,
            "file_path": file_metadata.file_path,
            "record_count": file_metadata.record_count,
            "create_date": file_metadata.create_date.isoformat() if file_metadata.create_date else None,
            "created_by": file_metadata.created_by,
            "external_file": file_metadata.external_file
        }

    @staticmethod
    def __object_metadata_to_dict(object_metadata: ObjectMetadata) -> dict:
        return {
            "object_name": object_metadata.object_name,
            "create_date": object_metadata.create_date.isoformat() if object_metadata.create_date else None,
            "file_count": object_metadata.file_count,
            "created_by": object_metadata.created_by,
            "file_format": object_metadata.file_format,
            "location": object_metadata.location,
            "file_metadata": [ObjectStore.__file_metadata_to_dict(fm) for fm in object_metadata.file_metadata],
            "update_date": object_metadata.update_date.isoformat() if object_metadata.update_date else None,
            "properties": object_metadata.properties
        }


class Operator(ABC):
    def __init__(self, op_type: str):
        self.__op_type = op_type

    def op_type(self) -> str:
        return self.__op_type


class RelationalOperator(Operator):
    def __init__(self, op_type: str, key: str, value: Any):
        super().__init__(op_type)
        self.key = key
        self.value = value


class LogicalOperator(Operator):
    def __init__(self, op_type: str, operators: List[Operator]):
        super().__init__(op_type)
        self.operators = operators


class Operators:

    @staticmethod
    def and_op(operators: List[Operator]) -> Operator:
        return LogicalOperator(op_type='and', operators=operators)

    @staticmethod
    def or_op(operators: List[Operator]) -> Operator:
        return LogicalOperator(op_type='or', operators=operators)

    @staticmethod
    def eq_op(key: str, value: Any):
        return RelationalOperator(key=key, value=value, op_type='eq')

    @staticmethod
    def neq_op(key: str, value: Any):
        return RelationalOperator(key=key, value=value, op_type='neq')


class Expression(ABC):

    @abstractmethod
    def get_expression(self) -> Callable[[Any], bool]:
        pass


class Visitor(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def visit(self, operator: Operator) -> Expression:
        pass


class DictExpression(Expression):

    def __init__(self, exp: Callable[[Any], bool]):
        self.__exp = exp

    def get_expression(self) -> Callable[[Any], bool]:
        return self.__exp


class DictVisitor(Visitor):

    def __init__(self):
        pass

    def visit(self, operator: Operator) -> DictExpression:
        if not operator:
            raise ValueError('operator can not be null')

        if isinstance(operator, LogicalOperator):
            return self.__parse_logical_op(operator)
        elif isinstance(operator, RelationalOperator):
            return DictVisitor.__parse_relational_op(operator)
        else:
            raise ValueError('operator tye not supported')

    def __parse_logical_op(self, operator: LogicalOperator) -> DictExpression:
        expressions = list(map(lambda op: self.visit(op), operator.operators))
        if operator.op_type() == 'and':
            result = lambda x: all(p(x) for p in expressions)
            return DictExpression(exp=result)
        elif operator.op_type() == 'or':
            result = lambda x: any(p(x) for p in expressions)
            return DictExpression(exp=result)
        else:
            raise ValueError(f'operator tye not supported- {operator.op_type()}')

    @staticmethod
    def __parse_relational_op(operator: RelationalOperator) -> DictExpression:
        if operator.op_type() == 'eq':
            result = lambda x: x.get(operator.key) == operator.value
            return DictExpression(exp=result)
        else:
            raise ValueError(f'operator tye not supported- {operator.op_type()}')


class DataIO(ABC):

    @abstractmethod
    def read_data(self, file_path: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def write_data(self, file_path: str, data: List[Dict[str, Any]]):
        pass


class JsonDataIO(DataIO):

    def __init__(self):
        pass

    def read_data(self, file_path: str) -> List[Dict[str, Any]]:
        return JsonFileUtil.read_file(file_path=file_path)

    def write_data(self, file_path: str, data: List[Dict[str, Any]]):
        JsonFileUtil.write_file(file_path=file_path, data=data)


class DataIOFactory:

    @staticmethod
    def get_data_io(provider: str) -> DataIO:
        if provider == 'json':
            return JsonDataIO()
        else:
            raise ValueError(f'unknown io provider- {provider}')


class QueryStatement:

    def __init__(self, object_name: str, object_store: ObjectStore):
        self.__object_name = object_name
        self.__object_store = object_store
        self.__visitor = DictVisitor()

    def query_all(self):
        pass

    def query(self, operator: Operator):
        if not operator:
            raise ValueError('operator can not be null')

        object_metadata = self.__object_store.get_object_metadata(object_name=self.__object_name)
        if not object_metadata:
            raise ValueError(f'metadata not found for object- {self.__object_name}')

        files = object_metadata.file_metadata
        if not files or len(files) == 0:
            return None
        provider = DataIOFactory.get_data_io(provider=object_metadata.file_format)
        expression = self.__visitor.visit(operator).get_expression()
        selected_elements = []
        for file in files:
            elements = provider.read_data(file_path=file.file_path)
            selected = list(filter(lambda element: expression(element), elements))
            if len(selected) > 0:
                selected_elements.extend(selected)

        return selected_elements

    def insert(self, data: Dict[str, Any]):
        self.insert_batch(data=[data])

    def insert_batch(self, data: List[Dict[str, Any]]):
        if not data or len(data) == 0:
            raise ValueError(f'data should not be null or empty')

        object_metadata = self.__object_store.get_object_metadata(object_name=self.__object_name)
        if not object_metadata:
            raise ValueError(f'metadata not found for object- {self.__object_name}')

        provider = DataIOFactory.get_data_io(provider=object_metadata.file_format)
        file_name = f'{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.{object_metadata.file_format}'
        file_path = os.path.join(object_metadata.location, file_name)

        provider.write_data(file_path=file_path, data=data)

        file_metadata = FileMetadata(
            file_name=file_name,
            file_path=file_path,
            record_count=len(data),
            create_date=datetime.now(),
            created_by='',
            external_file=False
        )

        self.__object_store.add_file(object_name=self.__object_name, file_metadata=file_metadata)


store = ObjectStore(store_path="D:\\dev\\iot_analysis\\test\\store\\")

# store.register_object(object_name='test_table', file_format='json', location="D:\\dev\\iot_analysis\\test\\store\\",
#                      properties={})

statement = store.query_object(object_name='test_table')

data = [
    {"type": "job"}
]
#statement.insert_batch(data=data)

result=statement.query(operator=Operators.eq_op(key='type', value="job"))
print(result)
