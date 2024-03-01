import time


def get_weight(limit: int) -> int:
    if limit < 100:
        return 1
    elif limit < 500:
        return 2
    elif limit <= 1000:
        return 5
    elif limit > 1000:
        return 10



class Counter:
    __slots__ = ["start", "weight", "weight_limit", "interval"]

    def __init__(self, weight_limit: int = 1200, interval: int = 60):
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
        str_ls = [f"time={now - self.start : .2f} weight_cost={self.weight} weight_limit={self.weight_limit}"]
        return "".join(str_ls)
