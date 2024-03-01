# This file contain filters for the rate limit of the Binance API.
import time
time_to_sec = {
    "SECOND": 1,
    "MINUTE": 60,
    "HOUR": 60 * 60,
}

class RequestRateLimiter(object):

    __slots__ = ["request_rate_limit_info", "start", "weight", "weight_limit", "interval"]

    def __init__(self, interval: int = 60, rate_limit_info=None):
        self.request_rate_limit_info = [info for info in rate_limit_info if info['rateLimitType'] == 'REQUEST_WEIGHT']
        self.start = time.time()
        self.weight = 0
        self.weight_limit = weight_limit
        self.interval = interval

    def add(self, weight):
        now = time.time()
        elapsed = now - self.start

        # 如果已经过了一分钟，就重置计数器
        if elapsed >= self.interval:
            self.start = now
            self.weight = 0

        # 累加weight
        self.weight += weight

        # 如果累积的weight超过了weight_limit，就等待到下一分钟
        if self.weight > self.weight_limit:
            time_to_next_minute = 60 - elapsed
            time.sleep(time_to_next_minute)
            self.start = time.time()
            self.weight = 0

    def __str__(self):
        now = time.time()
        str_ls = []
        # str_ls.append(f"start time  : {self.start}\n")
        # str_ls.append(f"now         : {now}\n")
        # str_ls.append(f"weight cost : {self.weight}\n")
        # if now - self.start == 0:
        #     avg_weight_cost = self.weight
        # else:
        #     avg_weight_cost = self.weight / (now - self.start) * 60
        # str_ls.append(f"avg weight cost : {avg_weight_cost}weights/min\n")
        str_ls.append(f"time={now - self.start : .2f} weight_cost={self.weight} weight_limit={self.weight_limit}")
        return "".join(str_ls)


class OrderRateLimiter(object):
    def __init__(self, rate_limit_info: dict):
        order_rate_limit_info = [info for info in rate_limit_info if info['rateLimitType'] == 'ORDERS']


    def new_order(self):
        return

    def modify_order(self):
        ...

import binance.client