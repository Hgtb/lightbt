import binance


class WeightTracker(object):
    def __init__(self):

        self.used_weight = 0
        self.order_count_10s: int = 0
        self.order_count_1m: int = 0
        self.weight_limit = 1200


class Client(binance.client.Client):
    def __init__(self):
        super().__init__()
        self.rate_limiter = WeightTracker()


