


class RequestRateLimiter(object):
    def __init__(self, rate_limit_info: list):
        request_rate_limit_info = [info for info in rate_limit_info if info['rateLimitType'] == 'REQUEST_WEIGHT']

        ...



class OrderRateLimiter(object):
    def __init__(self, rate_limit_info: dict):
        order_rate_limit_info = [info for info in rate_limit_info if info['rateLimitType'] == 'ORDERS']


    def new_order(self):
        return

    def modify_order(self):
        ...

import binance.client