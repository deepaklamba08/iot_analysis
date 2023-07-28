from abc import ABC, abstractmethod


class Entity(ABC):

    def __init__(self, object_id: str, name: str, status: bool, description: str = None, config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.description = description
        self.config = config


class Source(Entity):
    def __init__(self, object_id: str, name: str, status: bool,
                 source_type: str,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.source_type = source_type
        self.description = description
        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class Transformation(Entity):
    def __init__(self, object_id: str, name: str, status: bool, transformation_type: str, description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.transformation_type = transformation_type
        self.description = description
        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class Action(Entity):
    def __init__(self, object_id: str, name: str, status: bool, action_type: str, description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.action_type = action_type
        self.description = description
        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class Application(Entity):
    def __init__(self, object_id: str, name: str, status: bool,
                 sources: list,
                 transformations: list,
                 actions: list,
                 description: str = None, config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.sources = sources
        self.transformations = transformations
        self.actions = actions
        self.description = description
        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class DataBag:

    def __init__(self, name, data, provider='unknown', metadata: dict = {}):
        self.name = name
        self.data = data
        self.provider = provider
        self.metadata = metadata

    def __str__(self):
        return f'[name = {self.name}, provider = {self.provider}]'


class SourceTemplate:

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load(self, **kwargs) -> DataBag:
        pass


class TransformationTemplate:

    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, **kwargs) -> DataBag:
        pass


class ActionTemplate:

    @abstractmethod
    def call(self, **kwargs):
        pass


class RuntimeContext:

    def __init__(self, parameters: dict):
        self.parameters = parameters

    def config_file(self) -> str:
        return self.parameters.get('config_file')

    def application_id(self) -> str:
        return self.parameters.get('app_id')

    def get_value(self, key: str):
        return self.parameters.get(key)

    def get_value(self, key: str, default):
        return self.parameters.get(key, default)
