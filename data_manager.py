import concurrent.futures
import logging
import pandas as pd
import requests
from binance.client import Client
from counter_base import Counter, get_weight
from counter_base import get_interval
from database import DatabaseManager
from dev_event_system import EventTypeDrivenEngine
from events import BarFinishedEvent
from requests import RequestException
from time import sleep, time_ns
from time_transform import timestamp_to_time
from tqdm import tqdm
from typing import Union, List, Tuple

from websocket_manager import WebsocketManager

database_path = 'database/crypto_data.db'
base_future_kline = "https://fapi.binance.com/fapi/v1/klines"

kline_data_order = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount', 'count', 'buy_amount',
                    'buy_volume']


def get_future_klines_base(params, headers, max_attempts=5, backoff_factor=2.0):
    """
    Attempts to download data with retries on failure.

    :param params: The parameters for the GET request.
    :param headers: The headers for the GET request.
    :param max_attempts: Maximum number of retry attempts.
    :param backoff_factor: Factor to determine the delay between attempts.
    :return: JSON response from the server.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            response = requests.get(base_future_kline, params=params, headers=headers)
            response.raise_for_status()  # This will raise an exception for HTTP error codes
            return response.json()
        except RequestException as e:
            attempt += 1
            wait_time = backoff_factor * (2 ** attempt)
            print(f"Request failed: {e}. Retrying in {wait_time} seconds.")
            sleep(wait_time)

    # If the loop completes without returning, all attempts have failed.
    raise Exception(f"Failed to download data after {max_attempts} attempts.")


def convert_str_to_number(data):
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = convert_str_to_number(value)
    elif isinstance(data, list):
        return [convert_str_to_number(item) for item in data]
    elif isinstance(data, str):
        try:
            # 尝试将字符串转换为float
            value = float(data)
            # 如果转换后的数是整数，则转换为int
            return int(value) if value.is_integer() else value
        except ValueError:
            return data
    return data


def adjust_time_period_to_frequency(start_time, end_time, interval_size):
    # 将start_time和end_time对其到interval对应的bar时间上
    if start_time % interval_size != 0:
        start_time = start_time - start_time % interval_size + interval_size
    if end_time % interval_size != 0:
        end_time = end_time - end_time % interval_size
    return start_time, end_time


def find_intervals(a: Tuple[int, int], b: Tuple[int, int]) -> list:
    # 找到a包含但b不包含的区间
    # 初始化结果列表
    result = []

    # a 和 b 的开始和结束点
    a_start, a_end = a
    b_start, b_end = b

    # 如果 a 完全在 b 的左边
    if a_end <= b_start:
        return [a]
    # 如果 a 完全在 b 的右边
    elif a_start >= b_end:
        return [a]
    # 如果 a 的左边部分被 b 覆盖
    elif a_start < b_start and a_end > b_start:
        result.append([a_start, b_start])
    # 如果 a 的右边部分被 b 覆盖
    if a_end > b_end and a_start < b_end:
        result.append([b_end, a_end])

    return result


def split_time_intervals(start_time: int, end_time: int, interval_size: int, chunk_size: int) -> List[Tuple[int, int]]:
    """
    Splits a large time interval into smaller intervals.
    :param start_time: The start time of the interval.
    :param end_time: The end time of the interval.
    :param interval_size: The size of each interval in seconds.
    :param chunk_size: The number of intervals between start_time and end_time
    :return: A list of tuples, where each tuple represents a smaller time interval.
    """
    intervals = []
    current_start = start_time
    while current_start < end_time:
        current_end = min(current_start + interval_size * chunk_size, end_time)
        intervals.append((current_start, current_end))
        current_start = current_end
    return intervals


class DataManager(object):
    def __init__(self, api_key: str, api_secret: str, db_path: str, event_engine: EventTypeDrivenEngine):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(logging.FileHandler("data_manager_logger.log", 'w'))

        self.api_key = api_key
        self.api_secret = api_secret
        self.event_engine: EventTypeDrivenEngine = event_engine
        self.dbm: DatabaseManager = DatabaseManager(db_path=db_path)
        self.wsm: WebsocketManager = WebsocketManager(api_key=api_key, api_secret=api_secret, event_engine=event_engine)
        self.client = None
        self.counter = Counter(1200)
        ...

    def _register_bar_finished_even(self):
        self.event_engine.register(event_type=BarFinishedEvent, handler=self._handle_bar_finished_even)

    def _unregister_bar_finished_even(self):
        self.event_engine.unregister(event_type=BarFinishedEvent, handler=self._handle_bar_finished_even)

    def _handle_bar_finished_even(self, even: BarFinishedEvent):
        finished_bar = self.wsm.get_data(timestamp=even.timestamp, interval=even.interval)
        self.dbm.insert_data(data_type="kline", frequency=even.interval, data=finished_bar)

    def _check_client(self, client: Client) -> Client:
        if client:
            return client
            # try:
            #     if client.ping():
            #         return client
            #     else:
            #         raise ValueError("Client out of time.")
            # except:
            #     client = Client(api_key=self.api_key, api_secret=self.api_secret)
            #     try:
            #         if client.ping():
            #             return client
            #         else:
            #             raise ValueError("Client out of time.")
            #     except Exception as e:
            #         print("Client Check Error:")
            #         print(e)
        else:
            return Client(api_key=self.api_key, api_secret=self.api_secret)

    def get_futures_exchange_info(self):
        """下载并整理futures和exchange的信息"""
        self.client = self._check_client(self.client)
        raw_futures_tickers = self.client.futures_ticker()
        raw_exchange_info = self.client.futures_exchange_info()
        self.counter.add(10)  # 消耗的权重不精确

        # 从exchange_info中提取symbols信息，并从原始字典中删除
        raw_symbols_info = raw_exchange_info.pop("symbols")

        # 将futures_tickers和symbols_info的列表转换为以symbol为键的字典
        raw_futures_tickers = {ticker.pop("symbol"): ticker for ticker in raw_futures_tickers}
        raw_symbols_info = {ticker.pop("symbol"): ticker for ticker in raw_symbols_info}

        # 获取futures_tickers和symbols_info中的共有symbols
        futures_tickers_symbols = [symbol for symbol, _ in raw_futures_tickers.items()]
        exchange_info_symbols = [symbol for symbol, _ in raw_symbols_info.items()]
        common_symbols = set(futures_tickers_symbols).intersection(set(exchange_info_symbols))

        common_orderTypes = None
        common_timeInForce = None
        futures_info = {}
        for symbol in common_symbols:
            # 对于每个共有symbol，找到其orderTypes和timeInForce的交集
            if common_orderTypes is None:
                common_orderTypes = set(raw_symbols_info[symbol]["orderTypes"])
            else:
                common_orderTypes.intersection(set(raw_symbols_info[symbol]["orderTypes"]))

            if common_timeInForce is None:
                common_timeInForce = set(raw_symbols_info[symbol]["timeInForce"])
            else:
                common_timeInForce.intersection(set(raw_symbols_info[symbol]["timeInForce"]))

            # 从futures_ticker中精简数据，保留weightedAvgPrice, volume, count
            future_info = {k: raw_futures_tickers[symbol][k] for k in ['weightedAvgPrice', 'volume', 'count']}

            # 从symbols_info中精简数据，除去orderTypes和timeInForce
            symbol_info = {k: raw_symbols_info[symbol][k] for k in raw_symbols_info[symbol].keys() if
                           k not in ['orderTypes', 'timeInForce']}

            # 将filters字段的列表转换为字典
            symbol_info["filters"] = {f['filterType']: {k: v for k, v in f.items() if k != 'filterType'} for f in
                                      symbol_info['filters']}

            # 合并future_info和symbol_info
            futures_info[symbol] = {**symbol_info, **future_info}

        # 更新exchange_info字典，添加orderTypes和timeInForce
        exchange_info = raw_exchange_info
        exchange_info["orderTypes"] = list(common_orderTypes)
        exchange_info["timeInForce"] = list(common_timeInForce)

        futures_info = convert_str_to_number(futures_info)
        exchange_info = convert_str_to_number(exchange_info)

        return futures_info, exchange_info

    def get_klines(self,
                   symbol: str,
                   interval: str,
                   start_time: int,
                   end_time: int,
                   max_limit: int,
                   apiKey: str,
                   counter=None):
        # start_time_ts = time_to_timestamp(start_time)
        # end_time_ts = time_to_timestamp(end_time)
        period = get_interval(interval=interval)

        params = {
            'symbol': symbol,
            'interval': interval,
        }
        headers = {
            'X-MBX-APIKEY': apiKey
        }
        klines = []
        while start_time < end_time:
            temp_time_ts = min(start_time + max_limit * period, end_time) - 1
            params["startTime"] = start_time
            params["endTime"] = temp_time_ts
            params["limit"] = int((temp_time_ts - start_time) / period) + 1
            if (counter is not None) and isinstance(counter, Counter):
                counter.add(get_weight(params["limit"]))
                self.logger.info(f"Counter for {symbol}: {counter}")
            self.logger.info(f"Time period {timestamp_to_time(start_time)} -- {timestamp_to_time(temp_time_ts)}")
            klines += get_future_klines_base(params=params, headers=headers)
            start_time = temp_time_ts + 1
        # ToDo 需要统一数据格式和类型
        df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'end_time', 'amount',
                                           'count', 'buy_amount', 'buy_volume', 'null']).astype('float')
        df["symbol"] = symbol
        df = df[kline_data_order]
        return df, (symbol, interval, start_time, end_time)

    def _generate_download_info(self, symbols: list, interval: str, start_time: int, end_time: int, max_limit=1000
                                ) -> List[Tuple[str, int, int]]:

        interval_size = get_interval(interval)  # Calculate interval size based on the max_limit
        start_time, end_time = adjust_time_period_to_frequency(start_time=start_time, end_time=end_time,
                                                               interval_size=interval_size)

        futures_info, _ = self.get_futures_exchange_info()

        download_info: List[Tuple[str, int, int]] = []
        for symbol in symbols:
            print(symbol)
            s_start_time, s_end_time = list(self.dbm.get_data_time_range(data_type="kline", frequency=interval,
                                                                         symbol=symbol).values())
            print(symbol, "1")
            if (s_start_time is None) or (s_end_time is None):
                start_time_ = max(futures_info[symbol]["onboardDate"], start_time)
                start_time_, end_time = adjust_time_period_to_frequency(start_time=start_time_, end_time=end_time,
                                                                        interval_size=interval_size)
                intervals = split_time_intervals(start_time=start_time_, end_time=end_time,
                                                 interval_size=interval_size, chunk_size=max_limit)
            else:
                intervals = []
                start_time_ = max(futures_info[symbol]["onboardDate"], start_time)
                start_time_, end_time = adjust_time_period_to_frequency(start_time=start_time_, end_time=end_time,
                                                                        interval_size=interval_size)
                for adjusted_start_time, adjusted_end_time in find_intervals(
                        (start_time_, end_time), (s_start_time, s_end_time)):
                    intervals.extend(
                        split_time_intervals(start_time=adjusted_start_time, end_time=adjusted_end_time,
                                             interval_size=interval_size, chunk_size=max_limit))
            download_info += [(symbol, int(st), int(et)) for st, et in intervals]
        return download_info

    def download_history_kline(self, symbols: list, interval: str, start_time: int, end_time: int, max_limit=1000,
                               max_workers: int = 4):
        """下载历史数据，可以接续database中的数据继续下载"""
        self.client = self._check_client(self.client)
        print(1)
        need_download_info = self._generate_download_info(symbols=symbols, start_time=start_time, end_time=end_time,
                                                          interval=interval)
        print(2)
        if len(need_download_info) == 0:
            return

        # 记录下载数据信息
        self.logger.info(f"Fetching target data in following:")
        for _s, _st, _et in need_download_info:
            self.logger.info(f"{_s}: [{_st}, {_et}]")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

            futures = [
                executor.submit(self.get_klines, symbol, interval, s_start_time, s_end_time, max_limit, self.api_key,
                                self.counter)
                for symbol, s_start_time, s_end_time in need_download_info]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                # print("future.result()[0]")
                # print(future.result()[0])
                self.dbm.insert_data(data_type="kline", frequency=interval, data=future.result()[0])
                self.logger.info(f"Finished fetching data for {future.result()[1]}")

    def get_realtime_kline(self, symbols: list, interval: str):
        """下载实时数据"""
        self.client = self._check_client(self.client)

        # 保留10s以上的时间确保websocket的第一个bar收到所有symbol的最后一条数据
        time = int(time_ns() // 1000)
        if time % 60000 > 50:
            sleep(10)

        self.wsm.start_future_kline(symbols=symbols, interval=interval)

    def synchronous_kline(self):
        """下载指定日期后的历史数据，并实时更新历史数据"""
        ...

    def get_data(self, data_type: str, interval: str, time: int = None,
                 start_time: int = None, end_time: int = None, symbols: Union[str, list] = None
                 ) -> pd.DataFrame:
        """获取指定数据"""
        if data_type != "kline":
            raise ValueError("Do not support other data type yet.")

        if time:
            start_time = end_time = time
        db_data = self.dbm.query_data(data_type=data_type, frequency=interval, symbol=symbols,
                                      start_time=start_time, end_time=end_time)
        db_data_start_time: int = min(db_data["timestamp"])
        db_data_end_time: int = max(db_data["timestamp"])

        if (db_data_end_time < end_time) and self.wsm.running:
            # wsm获取实时数据, 已有数据从db中获取，没有的数据尝试从wsm中获取，并拼接到一个dataframe中
            ...
        else:
            # 考虑报错，或什么都不做，在获取实时数据时只获取截至到最新bar（可能不完整）的数据
            ...
        return

    def get_lasting_data(self, data_type: str, interval: str, time: int = None,
                         start_time: int = None, end_time: int = None, symbols: Union[str, list] = None,
                         n_bars: int = 1
                         ) -> pd.DataFrame:
        return

    def stop(self):
        if self.wsm.running:
            self.wsm.stop()
