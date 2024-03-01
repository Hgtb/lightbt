import enum
from binance.client import Client


# class Instrument(enum.IntEnum):
#     FUTURE = 0
#     ETF = 1


class Side(enum.Enum):
    SELL = Client.SIDE_SELL
    BUY = Client.SIDE_BUY
    ASK = SELL
    BID = BUY
    A = SELL
    B = BUY


class PositionSide(enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    BOTH = "BOTH"


class FutureOrderType(enum.Enum):
    MARKET = Client.FUTURE_ORDER_TYPE_MARKET
    LIMIT = Client.FUTURE_ORDER_TYPE_LIMIT
    STOP = Client.FUTURE_ORDER_TYPE_STOP
    STOP_MARKET = Client.FUTURE_ORDER_TYPE_STOP_MARKET
    TAKE_PROFIT = Client.FUTURE_ORDER_TYPE_TAKE_PROFIT
    PROFIT_MARKET = Client.FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET
    LIMIT_MAKER = Client.FUTURE_ORDER_TYPE_LIMIT_MAKER


class Lifespan(enum.Enum):
    FILL_AND_KILL = Client.TIME_IN_FORCE_FOK  # Fill and kill orders trade immediately if possible, otherwise they are cancelled
    GOOD_FOR_DAY = Client.TIME_IN_FORCE_GTC  # Good for day orders remain in the market until they trade or are explicitly cancelled
    IMMEDIATE_OR_CANCEL = Client.TIME_IN_FORCE_IOC


class KlineInterval(enum.Enum):
    MIN_1 = '1m'
    MIN_3 = '3m'
    MIN_5 = '5m'
    MIN_15 = '15m'
    MIN_30 = '30m'
    HOUR_1 = '1h'
    HOUR_2 = '2h'
    HOUR_4 = '4h'
    HOUR_6 = '6h'
    HOUR_8 = '8h'
    HOUR_12 = '12h'
    DAY_1 = '1d'
    DAY_3 = '3d'
    WEEK_1 = '1w'

    # interval str -> interval size
    def to_seconds(self):
        interval_map = {'s': 1,
                        'm': 60,
                        'h': 60 * 60,
                        'd': 24 * 60 * 60,
                        'w': 7 * 24 * 60 * 60}
        try:
            return int(self.value[:-1]) * interval_map[self.value[-1]]
        except KeyError:
            raise ValueError(f"Unknown unit in KlineInterval: {self.value[-1]}")

    def to_ms(self):
        return int(self.to_seconds() * 1000)

    to_timestamp = to_ms
    size = property(to_timestamp)

    def __str__(self):
        return self.value


# volumeΪĿ����ҽ�����(Target Currency, e.p. BTC, ETH)��amountΪ�Ƽۻ��ҽ��׶�(USDT)
kline_data_columns = [
    'timestamp',   # ʱ���
    'symbol',      # ���׶Ա�ʶ
    'open',        # ���̼�
    'high',        # ��߼�
    'low',         # ��ͼ�
    'close',       # ���̼�
    'volume',      # �ɽ���
    'amount',      # �ɽ���
    'count',       # �ɽ�����
    'buy_volume'   # �򵥳ɽ���
    'buy_amount',  # �򵥳ɽ���
]

kline_data_dtype = {
    'timestamp': 'int64',     # ʱ���
    'symbol': 'str',          # ���׶Ա�ʶ
    'open': 'float32',        # ���̼�
    'high': 'float32',        # ��߼�
    'low': 'float32',         # ��ͼ�
    'close': 'float32',       # ���̼�
    'volume': 'float32',      # �ɽ���
    'amount': 'float32',      # �ɽ���
    'count': 'int32',         # �ɽ�����
    'buy_volume': 'float32',  # �򵥳ɽ���
    'buy_amount': 'float32',  # �򵥳ɽ���
}

# print(KlineInterval.DAY_1)
# print(isinstance(KlineInterval.DAY_1, KlineInterval))
# print(KlineInterval.DAY_1.value)
# print({"interval": KlineInterval.DAY_1})
# print(KlineInterval.DAY_1.to_seconds())
# print(KlineInterval.DAY_1.to_ms())
#
# print(KlineInterval.DAY_1.to_timestamp())
# print(KlineInterval.DAY_1.size)
