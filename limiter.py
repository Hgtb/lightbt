# This file contain filters for the rate limit of the Binance API.
import time
time_to_sec = {
    "SECOND": 1,
    "MINUTE": 60,
    "HOUR": 60 * 60,
}

class RequestRateLimiter(object):

    __slots__ = ["request_rate_limit_info", "start", "weight", "weight_limit", "interval"]
    """
    {
      "rateLimitType": "REQUEST_WEIGHT",
      "interval": "MINUTE",
      "intervalNum": 1,
      "limit": 2400
    }
    """
    def __init__(self, interval: int = 60, weight_limit: int = 1200):
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
    """
    {
      "rateLimitType": "ORDERS",
      "interval": "MINUTE",
      "intervalNum": 1,
      "limit": 1200
    },
    {
      "rateLimitType": "ORDERS",
      "interval": "SECOND",
      "intervalNum": 10,
      "limit": 300
    }
    """
    def __init__(self):
        self.order_weight_limit: int = 1200
        self.order_weight_interval: int = 60

        self.order_num_limit: int = 10
        self.order_num_interval: int = 1

        self.operate_events =


    def new_order(self):
        return

    def modify_order(self):
        ...

import binance.client