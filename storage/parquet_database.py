import logging
import os
import pandas as pd
import pathlib
import pyarrow.parquet as pq
from pyarrow import dataset as ds
import re
from typing import Union, List
# from dask.dataframe import read_parquet
# import dask.dataframe as dd
from btlib.storage.database_abs import DatabaseBase
from multiprocessing import cpu_count

class ParquetDatabase(DatabaseBase):

    def __init__(self, path: Union[str, pathlib.Path]):
        self.path = path if isinstance(path, pathlib.Path) else pathlib.Path(path)
        assert self.path.is_dir(), AssertionError(f"Parquet database path is not a dir.")
        self.logger = logging.getLogger(self.__class__.__name__)
        self.files_name = self.__get_all_parquet_file_name()


    def __get_all_parquet_file_name(self) -> list:
        return [file_name
                for file_name in os.listdir(self.path)
                if file_name.endswith(".parquet")]

    def __select_file_name(self,
                           symbol: Union[str, List[str]] = None,
                           data_type: str = None,
                           interval: str = None
                           ) -> str:
        # Prepare the symbol condition for efficient lookup
        if isinstance(symbol, list):
            symbol_set = set(symbol)  # Convert list to set for O(1) lookups
        elif isinstance(symbol, str):
            symbol_set = {symbol}
        else:
            symbol_set = None

        # Helper function to check if a file name meets the conditions
        def meets_conditions(file_name: str) -> bool:
            parts = re.split(r'_|\.', file_name)  # Split the filename into parts

            if len(parts) != 4:  # Skip if the filename format is unexpected
                return False

            symbol_part, data_type_part, interval_part, _ = parts
            if symbol_set and symbol_part not in symbol_set:
                return False
            if data_type and data_type != data_type_part:
                return False
            if interval and interval != interval_part:
                return False

            return True

        # Filter file names in a single pass
        filtered_file_name = [file_name for file_name in self.files_name if meets_conditions(file_name)]

        return filtered_file_name

    def insert_data(self, data_type: str, interval: str, data: pd.DataFrame, append: bool = True, symbol: str = None):
        symbol = symbol if symbol else data["symbol"][0].values
        file_path = os.path.join(self.path, f"{symbol}_{data_type}_{interval}.parquet")

        if os.path.isfile(file_path) and append:
            # 文件存在且append标志为True，读取现有数据
            try:
                existing_data = pd.read_parquet(file_path)
                # 合并新旧数据，并根据需要的列去除重复项，这里假设以'timestamp'列为基准去重
                combined_data = pd.concat([existing_data, data]).drop_duplicates(subset=['timestamp', 'symbol'])
                # 将合并后的数据写回文件
                combined_data.to_parquet(file_path, engine='fastparquet', index=False)
            except Exception as e:
                self.logger.error(f"Error reading or writing to {file_path}: {e}")
        else:
            # 文件不存在或append标志为False，直接写入新数据
            try:
                data.to_parquet(file_path, engine='fastparquet', index=False)
            except Exception as e:
                self.logger.error(f"Error writing to {file_path}: {e}")

    #
    def query_data(self,
                   data_type: str, interval: str, symbol: Union[str, List[str]] = None,
                   time: int = None,
                   start_time: int = 0,
                   end_time: int = 1e14,
                   columns: Union[str, List[str]] = None
                   ) -> pd.DataFrame:
        self.files_name = self.__get_all_parquet_file_name()
        filtered_file_name = self.__select_file_name(symbol=symbol, data_type=data_type, interval=interval)
        filtered_file_paths = [os.path.join(self.path, file_name) for file_name in filtered_file_name]

        # Define the filter condition

        filter_condition = None
        if time:
            # If a specific timestamp is given, use it for both start and end time
            filter_condition = ds.field('timestamp').between(time, time)
        else:
            # Construct filter conditions based on the presence of start_time and/or end_time
            if start_time and end_time:
                filter_condition = (ds.field('timestamp') >= start_time) & (ds.field('timestamp') <= end_time)
            elif start_time:
                filter_condition = (ds.field('timestamp') >= start_time)
            elif end_time:
                filter_condition = (ds.field('timestamp') <= end_time)

        # Prepare columns parameter
        if columns:
            if isinstance(columns, str):
                columns = [columns]  # Ensure columns is a list if only one column is given

        # Use PyArrow Dataset API for efficient multi-file reading
        try:
            dataset = ds.dataset(filtered_file_paths, format="parquet", partitioning="hive")
            table = dataset.to_table(filter=filter_condition, columns=columns)
            final_df = table.to_pandas()
        except Exception as e:
            print(f"Error reading dataset: {e}")
            final_df = pd.DataFrame()

        return final_df

    def get_time_range(self, data_type: str, interval: str, symbol: str = None) -> tuple:
        timestamps = self.query_data(data_type=data_type, interval=interval, symbol=symbol, columns="timestamp")
        return timestamps.min().iloc[0], timestamps.max().iloc[0]

    def get_time_range_fast(self, data_type: str, interval: str, symbol: str = None) -> tuple:
        # 初始化最大值和最小值
        global_min = None
        global_max = None
        file_paths = self.__select_file_name(data_type=data_type, interval=interval, symbol=symbol)
        # 遍历每个文件
        for file_path in file_paths:
            # 读取Parquet文件
            parquet_file = pq.ParquetFile(os.path.join(self.path, file_path))
            # 遍历每个row group的统计信息
            for rg in parquet_file.metadata.to_dict()["row_groups"]:
                # 获取特定列的统计信息
                col_metadata = rg["columns"]
                col_stats = None
                for col in col_metadata:
                    if col["path_in_schema"] == "timestamp":
                        col_stats = col["statistics"]

                # 更新全局最小值和最大值
                if col_stats and col_stats.get("has_min_max"):
                    local_min = col_stats["min"]
                    local_max = col_stats["max"]

                    global_min = local_min if global_min is None else min(global_min, local_min)
                    global_max = local_max if global_max is None else max(global_max, local_max)
        return global_min, global_max

    def get_columns(self, data_type: str, interval: str, symbol: str = None) -> dict:
        file_name = self.__select_file_name(data_type=data_type, interval=interval, symbol=symbol)
        if file_name:
            parquet_file = pq.ParquetFile(os.path.join(self.path, file_name[0]))
            return parquet_file.schema.names
        return None

    def get_symbols_list(self, data_type: str, interval: str) -> list:
        file_name = self.__select_file_name(data_type=data_type, interval=interval)
        return [fn.rsplit('_', 3)[0] for fn in file_name]
