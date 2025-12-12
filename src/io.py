from datetime import datetime


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
                 file_metadata: list[FileMetadata] = [],
                 update_date: datetime = None,
                 properties: dict = {}):
        self.object_name = object_name
        self.create_date = create_date
        self.file_count = file_count
        self.created_by = created_by
        self.file_format = file_format
        self.file_metadata = file_metadata
        self.update_date = update_date
        self.properties = properties


class QueryStatement:

    def __init__(self):
        pass


class ObjectStore:

    def __init__(self, store_path):
        self.store_path = store_path

    def register_object(self, object_name: str, file_format: str, properties: dict = {}):
        pass

    def list_objects(self):
        pass

    def get_object_metadata(self, object_name: str) -> ObjectMetadata:
        pass

    def add_file(self, object_name: str, file_metadata: FileMetadata):
        pass

    def query_object(self, object_name: str) -> QueryStatement:
        pass
