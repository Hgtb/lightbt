import pandas as pd
from abc import ABC, abstractmethod


class Event(ABC):
    @abstractmethod
    def __init__(self):
        ...

    def __repr__(self):
        if hasattr(self, '__slots__'):
            attributes = [(slot, getattr(self, slot)) for slot in self.__slots__]
        else:
            attributes = vars(self).items()
        attrs_str = ", ".join([f"{key}={value}" for key, value in attributes])
        return f"{self.__class__.__name__}({attrs_str})"


class InstrumentBarEvent(Event):
    _slots__ = ("timestamp", "symbol")

    def __init__(self, timestamp: int, symbol: str):
        self.timestamp: int = timestamp
        self.symbol: str = symbol


class BarFinishedEvent(Event):
    __slots__ = ("timestamp", "interval", "data")

    def __init__(self, timestamp: int, interval: str, data: pd.DataFrame):
        self.timestamp: int = timestamp
        self.interval: str = interval
        self.data: pd.DataFrame = data


class TimerEvent(Event):
    __slots__ = ("timestamp", "interval")

    def __init__(self, timestamp: int = None, interval: int = None):
        self.timestamp: int = timestamp
        self.interval: int = interval


class NewPositionEvent(Event):
    __slots__ = ("timestamp", "position")

    def __init__(self, positions: pd.DataFrame, timestamp: int = None):
        self.timestamp: int = timestamp
        self.position: pd.DataFrame = positions


if __name__ == "__main__":
    te = TimerEvent(123456, 60)
    print(te)
    print(te.__class__.__name__)
