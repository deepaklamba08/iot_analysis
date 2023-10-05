from abc import ABC, abstractmethod


class Entity(ABC):

    def __init__(self, object_id: str,
                 name: str,
                 status: bool,
                 create_date: str = None,
                 update_date: str = None,
                 created_by: str = None,
                 updated_by: str = None,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.create_date = create_date
        self.update_date = update_date
        self.created_by = created_by
        self.updated_by = updated_by
        self.description = description
        self.config = config


class Source(Entity):
    def __init__(self, object_id: str, name: str, status: bool,
                 source_type: str,
                 create_date: str = None,
                 update_date: str = None,
                 created_by: str = None,
                 updated_by: str = None,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.source_type = source_type
        self.create_date = create_date
        self.update_date = update_date
        self.created_by = created_by
        self.updated_by = updated_by
        self.description = description

        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class Transformation(Entity):
    def __init__(self, object_id: str, name: str, status: bool, transformation_type: str,
                 create_date: str = None,
                 update_date: str = None,
                 created_by: str = None,
                 updated_by: str = None,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.transformation_type = transformation_type
        self.create_date = create_date
        self.update_date = update_date
        self.created_by = created_by
        self.updated_by = updated_by
        self.description = description
        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class Action(Entity):
    def __init__(self, object_id: str, name: str, status: bool, action_type: str,
                 create_date: str = None,
                 update_date: str = None,
                 created_by: str = None,
                 updated_by: str = None,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.action_type = action_type
        self.create_date = create_date
        self.update_date = update_date
        self.created_by = created_by
        self.updated_by = updated_by
        self.description = description
        self.config = config

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"


class Application(Entity):
    def __init__(self, object_id: str, name: str, status: bool,
                 sources: list,
                 transformations: list,
                 actions: list,
                 create_date: str = None,
                 update_date: str = None,
                 created_by: str = None,
                 updated_by: str = None,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.sources = sources
        self.transformations = transformations
        self.actions = actions
        self.description = description
        self.create_date = create_date
        self.update_date = update_date
        self.created_by = created_by
        self.updated_by = updated_by
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


class DatabagLookup:

    def __init__(self, src_data_bags: dict, tr_data_bags: dict):
        self.src_data_bags = src_data_bags
        self.tr_data_bags = tr_data_bags

    def all_sources_databags(self):
        return self.src_data_bags

    def all_transformation_databags(self):
        return self.tr_data_bags

    def get_databag(self, name: str, is_source: bool) -> DataBag:
        if is_source:
            databag = self.src_data_bags.get(name)
        else:
            databag = self.tr_data_bags.get(name)

        if not databag:
            raise Exception(f'databag not found - {name}')

        return databag


class DatabagRegistry:

    def __init__(self):
        self.src_data_bags = {}
        self.tr_data_bags = {}

    def get_lookup(self) -> DatabagLookup:
        return DatabagLookup(self.src_data_bags, self.tr_data_bags)

    def source_databag(self, name: str, databag: DataBag):
        if name in self.src_data_bags.keys():
            raise Exception(f'source databag already exists by name - {name}')
        self.src_data_bags[name] = databag

    def transformation_databag(self, name: str, databag: DataBag):
        if name in self.tr_data_bags.keys():
            raise Exception(f'transformation databag already exists by name - {name}')
        self.tr_data_bags[name] = databag

    def __str__(self):
        return f"[total data bags = {len(self.src_data_bags) + len(self.tr_data_bags)}]"


class SourceTemplate:

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load(self, **kwargs) -> DataBag:
        pass


class TransformationTemplate:

    def __init__(self, databag_lookup: DatabagLookup):
        self.databag_lookup = databag_lookup

    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def execute(self, **kwargs) -> DataBag:
        pass


class ActionTemplate:

    def __init__(self, databag_lookup: DatabagLookup):
        self.databag_lookup = databag_lookup

    @abstractmethod
    def call(self, **kwargs):
        pass


class RuntimeContext:

    def __init__(self, parameters: dict):
        self.parameters = parameters

    def config_file(self) -> str:
        return self.parameters.get('config_file')

    def execution_summary_dir(self) -> str:
        return self.parameters.get('execution_summary_dir')

    def job_id(self) -> str:
        return self.parameters.get('app_id')

    def get_value(self, key: str):
        return self.parameters.get(key)

    def get_value(self, key: str, default):
        return self.parameters.get(key, default)


class Job(Entity):
    def __init__(self, object_id: str, name: str, status: bool,
                 application_id: str,
                 create_date: str = None,
                 update_date: str = None,
                 created_by: str = None,
                 updated_by: str = None,
                 description: str = None,
                 config: dict = {}):
        self.object_id = object_id
        self.name = name
        self.status = status
        self.application_id = application_id
        self.create_date = create_date
        self.update_date = update_date
        self.created_by = created_by
        self.updated_by = updated_by
        self.description = description
        self.config = config

    def job_parameters(self) -> dict:
        return self.config.get('parameters', {})

    def __str__(self):
        return f"[object_id = {self.object_id}, name = {self.name}]"
