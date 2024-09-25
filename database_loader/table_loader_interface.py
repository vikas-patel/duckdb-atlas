from abc import ABC, abstractmethod

class TableLoaderInterface(ABC):
    @abstractmethod
    def set_file_path(self, file_path: str):
        pass

    @abstractmethod
    def create_table_from_schema(self):
        pass

    @abstractmethod
    def fast_load_data_from_csv(self):
        pass

    @abstractmethod
    def execute_sql(self, query: str):
        pass