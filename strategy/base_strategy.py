import abc
import logging
import pandas as pd
from typing import List, Optional

from btlib.event import BarFinishedEvent, TimerEvent, NewPositionEvent
from btlib.event import EventTypeDrivenEngine


# from .strategy_parameter import StrategyParameter

#
# class DataBuffer:
#     def __init__(self, bar_size, n_bar, columns):
#         self._data = pd.DataFrame()


class BaseStrategy(abc.ABC):
    def __init__(self, event_engine: Optional[EventTypeDrivenEngine] = None):
        self.event_engine = event_engine
        self.logger = logging.getLogger(self.__class__.__name__)
        self._data: pd.DataFrame = None
        self.time_counter: int = 0

    def _register_events(self):
        if self.event_engine:
            self.logger.debug("REGISTER EVENTS")
            self.event_engine.register(event_type=BarFinishedEvent, handler=self.on_event)
            self.event_engine.register(event_type=TimerEvent, handler=self.on_event)

    def _unregister_event(self):
        if self.event_engine:
            self.logger.debug("UNREGISTER EVENTS")
            self.event_engine.unregister(event_type=BarFinishedEvent, handler=self.on_event)
            self.event_engine.unregister(event_type=TimerEvent, handler=self.on_event)

    def start(self):
        self._register_events()

    def stop(self):
        self._unregister_event()

    def on_event(self, event):
        self.logger.debug(event)
        if isinstance(event, BarFinishedEvent):
            self.on_bar_finished_event(event)

    def update_data(self, data: pd.DataFrame):
        self.logger.debug("RECEIVE DATA")
        if self._data is not None:
            ts_ls = list(self._data["timestamp"].unique())
            if len(ts_ls) == self.data_length:
                # 删除 "timestamp" 列值等于最小 "timestamp" 值的行
                min_ts = min(ts_ls)
                self._data = self._data[self._data["timestamp"] != min_ts]
            self._data = pd.concat([self._data, data])
        else:
            self._data = data
        self.time_counter += len(data["timestamp"].unique())
        self.logger.debug(self._data)

    def on_bar_finished_event(self, event: BarFinishedEvent) -> None:
        self.update_data(event.data)
        if self.time_counter == self.strategy_frequency:
            target_position = self.execute(self._data)
            self.event_engine.put(NewPositionEvent(positions=target_position))
            self.time_counter = 0

    @abc.abstractmethod
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        return NotImplementedError

    @property
    @abc.abstractmethod
    def data_type(self) -> str:
        return NotImplementedError

    @property
    @abc.abstractmethod
    def data_interval(self) -> str:
        return NotImplementedError

    @property
    @abc.abstractmethod
    def data_length(self) -> int:
        return NotImplementedError

    @property
    @abc.abstractmethod
    def data_symbols(self) -> List[str]:
        return NotImplementedError

    @property
    @abc.abstractmethod
    def strategy_frequency(self) -> str:
        return NotImplementedError

    @property
    @abc.abstractmethod
    def strategy_frequency(self) -> str:
        return NotImplementedError
