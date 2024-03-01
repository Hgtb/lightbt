from btlib.event import EventTypeDrivenEngine, Event
import pandas as pd
from btlib.time_utils import now_ts


class AdjustPositionEvent(Event):

    def __init__(self, target_position: pd.DataFrame):
        self.target_position = target_position
        self.timestamp = now_ts()
