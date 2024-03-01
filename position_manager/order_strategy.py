import asyncio
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import logging
import queue
import threading
import time
from typing import Dict, List
from binance.client import AsyncClient
from binance.streams import ThreadedWebsocketManager
from btlib.bt_types import Side, PositionSide, FutureOrderType, Lifespan

import asyncio
import random


class Order(object):
    __slots__ = ("symbol", "quantity", "side", "type", "life_span", "position_side","price",
                 "execute_times", "amount")

    def __init__(self,
                 symbol: str,
                 quantity: float,

                 side: Side,
                 order_type: FutureOrderType,
                 amount: float = None,
                 life_span: Lifespan= Lifespan.GOOD_FOR_DAY,
                 position_side: PositionSide = PositionSide.BOTH,
                 callbackRate: float = None,
                 price: float = None,
                 ):

        if (order_type == FutureOrderType.LIMIT) and (not price):
            raise ValueError("Limit order should set `price`.")
        if callbackRate:
            assert (callbackRate >= 0.1) and (callbackRate <= 5), ValueError(
                f"Order parameter 'callbackRate' = {callbackRate} out of range [0.1, 5]")


        self.execute_times: int = 0
        self.amount: float = amount
        self.order_id: str = f"{symbol}"


        self.price: float = price
        self.symbol: str = symbol
        self.quantity: float = quantity
        self.side: Side = side
        self.type: FutureOrderType = order_type
        self.life_span: Lifespan = life_span
        self.position_side: PositionSide = position_side

    def update_quantity(self, new_quantity: float):
        self.quantity = new_quantity

    def update_price(self, new_price: float):
        self.price = new_price

    def to_market(self):
        self.type = FutureOrderType.MARKET
        self.price = None

    def to_limit(self, price: float):
        self.type = FutureOrderType.LIMIT
        self.price = price

    @property
    def params(self):
        return {"symbol": self.symbol,
                "side": self.side.value,
                "positionSide": self.position_side.value,
                "type": self.type.value,
                "quantity": self.quantity.value,
                }


class OrderFactory(object):
    def __init__(self):
        ...

    def market_price_order(self) -> Order:
        return

    def limit_price_order(self) -> Order:
        return


class AsyncOrderManager:
    # _instance = None


    def __init__(self, symbols, async_client: AsyncClient, twm: ThreadedWebsocketManager):
        self.symbols = symbols
        self._lock = threading.Lock()
        self.client = async_client
        self.order_queue = asyncio.Queue()
        self.order_pool: Dict[str, str] = {s: None for s in symbols}  # Save order id for each symbols
        self.book_depth: Dict[str, dict] = {s: None for s in symbols}
        self.order_status: Dict[str, bool] = {s: False for s in symbols}  # 记录订单是否正在执行
        self.order_times: Dict[str, int] = {s: 0 for s in symbols}  # 记录下订单的次数
        self.running = threading.Event()  # 控制运行的标志
        self.task = None
        self.twm = twm

    def on_book_depth(self, msg: dict):
        # User need to implement.
        with self._lock:
            if msg["e"] == "depthUpdate":
                self.book_depth[msg["s"]] = ...

    def on_new_position(self, position: dict):
        with self._lock:
            ...

    # 根据仓位差异与本金生成交易数据
    # 价格变动时，使用百分比仓位计算下单数量可能导致频繁调仓（?）
    # 执行部分根据交易数据产生订单，各symbol有一个异步任务 or 若有订单放入queue由异步执行函数统一执行
    # 执行部分需要根据深度数据 or 逐笔数据 填入订单价格 or 数量 等信息，产生订单，并执行;
    # 执行部分使用一个循环监控订单状态，若超时或失败则需要取消旧订单，重新生成订单数据并重新下订单，若失败次数过多则使用市价单强制成交
    # 注意订单数量limit与订单成交率限制
    def generate_orders(self) -> bool:
        # Generate some orders params for all symbols.
        # Generate amount for orders
        with self.running.is_set():
            for symbol in self.symbols:
                if self.order_status.get(symbol, False):  # If order for this symbol is executing.
                    continue


            ...  # Do something to generate order: Order

            # await asyncio.sleep(random.uniform(0.1, 0.5))  # 模拟生成订单的延时
            # for order in orders:
            #     await self.order_queue.put(order)  # 将订单放入队列
        return True
        # return {"symbol": symbol, "type": "buy", "price": 100, "quantity": 1}

    # def update_order_params(self, order):
    #     # update
    #     return order

    async def execute_order(self, order_params,
                            max_try_order_times: int = 5,
                            order_waiting_time: float = 1.
                            ):
        # 模拟执行订单的异步操作
        # Calculate quantity based on amount and order price
        # order price is depend on deep data
        # User could change this function.

        times: int = 0
        order_id: str = None
        while self.running.is_set():

            if self.order_queue.empty():
                asyncio.sleep(1.)
                continue
            if times < max_try_order_times:  # Send limit price order
                # Calculate quantity and send order

                await self.client.futures_create_order()
                await asyncio.sleep(order_waiting_time)

                # Check order status
                if ...:  # If order filled
                    ...
                    # self.order_status[symbol] = False
                else:    # If order not filled or partly filled, and best price changed
                    # Cancel the order
                    # Compute remain amount different
                    # What if remain amount < 5 USDT ?
                    times += 1
            else:  # Send market price order
                # order.
                await self.client.futures_create_order()

    async def manage_orders(self):
        while self.running.is_set():
            # 使用异步生成和执行订单
            orders = self.generate_orders()
            for order in orders:
                await self.order_queue.put(order)  # 将订单放入队列

            while not self.order_queue.empty():
                order = await self.order_queue.get()
                await self.execute_order(order)  # 异步执行订单
                self.order_queue.task_done()
            await asyncio.sleep(1)

    async def run(self):
        depth_streams = [f"{s}@depth5@250" for s in self.symbols]
        self.twm.start_futures_multiplex_socket(callback=self.on_book_depth, streams=depth_streams)
        self.task = asyncio.create_task(self.manage_orders())

        # Start the order generation in a separate thread
        threading.Thread(target=self.generate_orders, daemon=True).start()

        # Start the asyncio event loop for order execution
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.execute_order())

    # async def

    async def stop(self):
        self.running.clear()
        if self.task:
            await self.task
