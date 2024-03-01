# 仓位管理、下单、监控
import threading

import binance.client
import logging
from btlib.event import EventTypeDrivenEngine, Event, NewPositionEvent
import pandas as pd
from btlib.time_utils import now_ts
from .position_events import AdjustPositionEvent
from btlib.websocket_manager import WebsocketManager
from typing import Dict
from threading import Event
import asyncio
from asyncio import Queue
from .order_strategy import AsyncOrderManager


class PositionManager(object):

    def __init__(self,
                 api_key, api_secret,
                 futures_info,
                 event_manager: EventTypeDrivenEngine,
                 websocket_manager: WebsocketManager,
                 symbols: list):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.trader = ...  # 根据订单下单，处理异常
        self.supervisor = ...  # 监控仓位状态、杠杆、获利亏损等信息
        self.futures_info = futures_info
        self.event_manager = event_manager
        self.loop = asyncio.get_event_loop()
        self.order_queue = Queue()

        self.symbols = symbols

        # self.best_price: pd.DataFrame = pd.DataFrame(columns=symbols, in)
        self.best_ask_price: Dict[str, float] = {s: None for s in symbols}
        self.best_ask_price: Dict[str, float] = {s: None for s in symbols}
        self.target_position = None
        self.target_position_update_time = None
        self.executing_order: Dict[str, bool] = {s: False for s in symbols}
        self.current_position = None

        self.wbm = websocket_manager
        self.client = binance.client.AsyncClient(api_key=api_key, api_secret=api_secret)
        self._active = Event()

        self.order_generator = DefaultOrderGenerator()
        self.order_executor = DefaultOrderExecutor()

    def register_events(self):
        self.event_manager.register(event_type=NewPositionEvent, handler=self.on_new_position)

    def unregister_events(self):
        self.event_manager.unregister(event_type=NewPositionEvent, handler=self.on_new_position)

    def start(self):
        self._active.set()
        self.loop.create_task(self.produce_orders())
        consumer_task = asyncio.create_task(self.consumer())
        # 启动其他需要异步运行的任务，例如websocket监听
        self.order_generator.start()

    def stop(self):
        self._active.clear()

    async def produce_orders(self):
        # 实时根据 new_position 与 当前仓位（从websocket监听获得）以及 self.best_ask_price, self.best_ask_price （从websocket实时监听并更新）放入下单策略函数判断是否下单，以及下单价格
        while self._active.is_set():
            self.time_limiter()

    async def manage_order(self, order):
        # 管理订单，包括超时重试逻辑
        try_count = 0
        while try_count < MAX_TRIES:
            success = await self.execute_order(order)
            if success:
                break
            try_count += 1
            await asyncio.sleep(RETRY_INTERVAL)  # 设置重试间隔

    async def consumer(self):
        while True:
            order = await self.order_queue.get()
            await self.manage_order(order)
            self.order_queue.task_done()

    def on_new_position(self, event: NewPositionEvent):
        if self.target_position_update_time and event.timestamp > self.target_position_update_time:
            self.target_position = event.position
            self.target_position_update_time = event.timestamp

    ...
