import logging
import time
from queue import Queue, Empty
import threading
from typing import Dict, List, Callable

from btlib.event.events import Event, TimerEvent, BarFinishedEvent


# 单例模式
class EventTypeDrivenEngine:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(EventTypeDrivenEngine, cls).__new__(cls)
        return cls._instance

    def __init__(self, max_queue: int = 100, timer_interval: int = 60):
        if not hasattr(self, '_initialized'):  # 防止__init__方法的重复调用
            self._queue = Queue(maxsize=max_queue)
            self._active = threading.Event()
            self._thread = threading.Thread(target=self._run, name="EventTypeDrivenEngine::ProcessThread")
            self._handlers: Dict[type, List[Callable]] = {}

            # self.register(BarFinishedEvent, self._handel_bar_finished_event)

            self._timer_interval = timer_interval
            self._timer = threading.Timer(interval=timer_interval, function=self._timer_handler)  # 不精准
            self._timer.setName("EventTypeDrivenEngine::Timer")
            self.logger = logging.getLogger(self.__class__.__name__)

    def _timer_handler(self):
        if self._active.is_set():
            self.put(TimerEvent(interval=self._timer_interval, timestamp=int(time.time_ns() // 1e6)))
            self._timer = threading.Timer(interval=self._timer_interval, function=self._timer_handler)
            self._timer.start()

    def _run(self):
        while self._active.is_set():
            try:
                event = self._queue.get(block=True, timeout=1)
                self.logger.debug("Get event from queue")
                self._process(event)
            except Empty:
                pass

    def _process(self, event):
        event_type = type(event)
        if event_type in self._handlers:
            for handler in self._handlers[event_type]:
                handler(event)

    def register(self, event_type: type, handler: Callable):
        self.logger.debug("REGISTER NEW EVENT")
        self.logger.debug(f"event_type={event_type}, handler={handler}")
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        self.logger.debug(f"{self._handlers}")

    def unregister(self, event_type: type, handler: Callable):
        if event_type in self._handlers:
            self._handlers[event_type].remove(handler)
            if not self._handlers[event_type]:
                del self._handlers[event_type]

    def put(self, event: Event):
        self.logger.debug(f"RECEIVE NEW EVENT: {type(event)}")
        self._queue.put(event, block=True)


    def start(self):
        self._active.set()
        if hasattr(self, "_timer") and self._timer:
            self._timer.start()
        self._thread.start()

    def stop(self):
        self._active.clear()
        if hasattr(self, "_timer") and self._timer:
            self._timer.cancel()
        self._thread.join()



if __name__ == "__main__":
    def handle_timer_event(event):
        print(f"处理计时器事件: {event}")


    def handle_timer_event2(event):
        print(f"处理计时器事件2: {event}")


    def handle_bar_finished_event(event):
        print(f"处理交易事件: {event}")


    engine = EventTypeDrivenEngine(1)
    engine.register(TimerEvent, handle_timer_event)
    engine.register(BarFinishedEvent, handle_bar_finished_event)
    engine.register(TimerEvent, handle_timer_event2)

    engine.start()
    # engine.put(TimerEvent(int(time.time_ns() // 1e6)))
    engine.put(BarFinishedEvent(123, "1m", 1))
    time.sleep(5)
    engine.put(BarFinishedEvent(456, "1m", 2))
    engine.unregister(TimerEvent, handle_timer_event)
    time.sleep(5)
    engine.put(BarFinishedEvent(789, "1m", 3))
    # time.sleep(1)
    engine.stop()
