import time


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


def get_interval(interval: str) -> int:
    # 输入interval(1s, 1m, 1d, ...), 返回interval的timestamp(ms)
    interval_map = {'s': 1000, 'm': 60 * 1000, 'h': 60 * 60 * 1000, 'd': 24 * 60 * 60 * 1000, "M": 30*24*60*60*1000}
    return int(interval[:-1]) * interval_map[interval[-1]]


def get_weight(limit: int) -> int:
    if limit < 100:
        return 1
    elif limit < 500:
        return 2
    elif limit <= 1000:
        return 5
    elif limit > 1000:
        return 10
