import logging
from typing import Callable

from .backtest import DefaultBacktest, BacktestAbstract
from .data_downloader import DataDownloader
from .resource_manager import ResourceManager
from .storage import ParquetDatabase
from .strategy import BaseStrategy


def setup_global_logger(filename=None, logger_level=logging.DEBUG):
    logger_level = logger_level
    filename = filename
    handlers = []
    if filename:
        handlers.append(logging.FileHandler(filename=filename, mode="w"))
    if logger_level == logging.DEBUG:
        handlers.append(logging.StreamHandler())
    # Set up basic configuration for logging
    logging.basicConfig(level=logger_level, handlers=handlers,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


setup_global_logger()

# 全局资源管理器实例
# if hasattr("_resource_manager"):
_resource_manager = ResourceManager()


def register_event_handler(event_type: type, handler: Callable):
    _resource_manager.register_event_handler(event_type=event_type, handler=handler)


def register_strategy(strategy):
    _resource_manager.register_strategy(strategy)


def register_data_manager(db_path: str, db_mode: str):
    _resource_manager.register_data_manager(db_path, db_mode)


def set_api(api_key: str, api_secret: str):
    _resource_manager.api_key = api_key
    _resource_manager.api_secret = api_secret


def set_database(db_path):
    ...


def add_strategy(UserStrategy):
    _resource_manager.register_strategy(UserStrategy)


def backtest(strategy: BaseStrategy = None,
             start_time: int = None,
             end_time: int = None,
             chunk_size: int = None,
             initial_capital: float = 1e5,  # USDT
             fee_rate: float = 5e-4,  # TAKER Fee Rate
             ):  # 进行回测
    _resource_manager.backtest(strategy=strategy, start_time=start_time, end_time=end_time, chunk_size=chunk_size,
                               initial_capital=initial_capital, fee_rate=fee_rate)
    return _resource_manager.backtest_impl


def start_trading():
    _resource_manager.start_trading()


def stop_trading():
    _resource_manager.start_trading()
