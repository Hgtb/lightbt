import pandas as pd
from abc import ABC, abstractmethod


class DatabaseBase(ABC):
    @abstractmethod
    def insert_data(self, data_type: str, interval: str, data: pd.DataFrame, append: bool = True):
        """
        Insert or append data for a given type and interval.
        """
        pass

    # @abstractmethod
    # def delete_data(self, data_type: str, interval: str, symbol: str = None, start_time: int = None,
    #                 end_time: int = None):
    #     """
    #     Delete data for a given type, interval, and optional symbol or time range.
    #     """
    #     pass

    @abstractmethod
    def query_data(self, data_type: str, interval: str, symbol: str = None,
                   time: int = None, start_time: int = None, end_time: int = None,
                   sort_by: str = None, ascending: bool = True,
                   page_number: int = None, page_size: int = None) -> pd.DataFrame:
        """
        Query data with various filters, sorting, and pagination options.
        """
        pass

    @abstractmethod
    def get_time_range(self, data_type: str, interval: str, symbol: str = None) -> dict:
        """
        Get the start and end time range for a given type, interval, and optional symbol.
        """
        pass

    @abstractmethod
    def get_columns(self, data_type: str, interval: str, symbol: str = None) -> dict:
        """
        Get the start and end time range for a given type, interval, and optional symbol.
        """
        pass

    @abstractmethod
    def get_symbols_list(self, data_type: str, interval: str) -> list:
        """
        Get a list of all symbols available for a given type and interval.
        """
        pass


    # @abstractmethod
    # def update_data(self, data_type: str, interval: str, data: pd.DataFrame):
    #     """
    #     Update existing data for a given type and interval.
    #     """
    #     pass

    # @abstractmethod
    # def save_to_file(self, data_type: str, interval: str, file_path: str):
    #     """
    #     Save data to a file for backup or analysis.
    #     """
    #     pass
    #
    # @abstractmethod
    # def load_from_file(self, file_path: str, data_type: str, interval: str):
    #     """
    #     Load data from a file into the database.
    #     """
    #     pass

