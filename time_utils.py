import time
from datetime import datetime
from typing import List, Tuple


def now_ts():
    return int(time.time() * 1000)


def time_to_timestamp(time_str: str, format_strs=('%Y-%m-%d %H:%M:%S', '%Y-%m-%d')):
    for format_str in format_strs:
        try:
            dt = datetime.strptime(time_str, format_str)
            timestamp = int(dt.timestamp() * 1000)  # Convert to milliseconds
            return timestamp
        except ValueError:
            pass
    raise ValueError(f"Time string {time_str} does not match any format.")


def timestamp_to_time(timestamp: int, format_str='%Y-%m-%d %H:%M:%S'):
    dt = datetime.fromtimestamp(timestamp / 1000)  # Convert from milliseconds
    return dt.strftime(format_str)


def adjust_time_period_to_frequency(start_time, end_time, interval_size):
    # 将start_time和end_time对其到interval对应的bar时间上
    if start_time and (start_time % interval_size != 0):
        start_time = start_time - start_time % interval_size + interval_size
    if end_time and (end_time % interval_size != 0):
        end_time = end_time - end_time % interval_size
    return start_time, end_time


def split_time_intervals(start_time: int, end_time: int, interval_size: int, chunk_size: int) -> List[Tuple[int, int]]:
    """
    Splits a large time interval into smaller intervals.
    :param start_time: The start time of the interval.
    :param end_time: The end time of the interval.
    :param interval_size: The size of each interval in ms.
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


def get_interval(interval: str) -> int:
    # 输入interval(1s, 1m, 1d, ...), 返回interval的timestamp(ms)
    interval_map = {'s': 1000, 'm': 60 * 1000, 'h': 60 * 60 * 1000, 'd': 24 * 60 * 60 * 1000}
    return int(interval[:-1]) * interval_map[interval[-1]]


