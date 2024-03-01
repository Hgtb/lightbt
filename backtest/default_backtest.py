import numpy as np
import pandas as pd
from numba import jit

from btlib.strategy import BaseStrategy
from btlib.storage import ParquetDatabase


# from .backtest_abstract import Bs

@jit(nopython=True)
def calculate_pnl_and_fee(positions: np.ndarray,
                          prices: np.ndarray,
                          initial_capital: float,
                          min_qty: np.ndarray,
                          step_size: np.ndarray,
                          notional: np.ndarray,
                          fee_rate: float):
    """
    计算PnL和手续费。

    参数:
    positions: numpy数组，表示每个symbol的目标仓位。
    prices: numpy数组，表示每个symbol的价格。
    initial_capital: float，初始资本。
    min_qty: numpy数组，每个symbol的最小成交量。
    step_size: numpy数组，每个symbol的成交量步长。
    notional: numpy数组，每个symbol的最小成交金额。
    fee_rate: float，手续费率。

    返回:
    pnl: numpy数组，表示每个时间点的累计PnL。
    fees: numpy数组，表示每个时间点的累计手续费。
    """
    num_symbols = positions.shape[1]
    num_times = positions.shape[0]

    current_capital: float = initial_capital
    current_position = np.zeros(num_symbols, dtype=np.float32)  # 各symbol持仓比例
    current_volume = np.zeros(num_symbols, dtype=np.float32)  # 各symbol持有数量

    # 根据第0天的position初始化持仓？
    trade_amount = round(positions[0] * initial_capital, 8)
    trade_volume = trade_amount / prices[0]

    trade_volume = np.round(trade_volume / step_size) * step_size  # 四舍五入的计算？
    trade_amount = np.round(trade_volume * prices[0], 8)  # 金额四舍五入到8位小数
    current_position = trade_amount / initial_capital
    current_volume = np.round(trade_volume / step_size) * step_size

    # position side
    # long_positions = np.zeros(shape=num_times, dtype=np.int32)
    # short_positions = np.zeros(shape=num_times, dtype=np.int32)

    # 初始化数组
    pnl = np.zeros(num_times, dtype=np.float)
    fees = np.zeros(num_times, dtype=np.float)

    for t in range(1, num_times):
        available_capacity = min(current_capital, initial_capital)  # 亏损不补仓
        # available_capacity = initial_capital  # 亏损补仓
        for symbol in range(num_symbols):
            # 计算交易量
            trade_positions = positions[t, symbol] - current_position[t - 1, symbol]
            trade_amount = round(trade_positions * available_capacity, 8)
            trade_volume = trade_amount / prices[t, symbol]

            # 确保交易量和交易金额符合交易所限制
            trade_volume = np.round(trade_volume / step_size[symbol]) * step_size[symbol]  # 四舍五入的计算？
            trade_amount = np.round(trade_volume * prices[t, symbol], 8)  # 金额四舍五入到8位小数

            # 更新PnL
            value_change = current_volume[symbol] * (prices[t][symbol] - prices[t - 1][symbol])
            pnl[t] += value_change
            current_capital += value_change

            # 如果不符合条件则跳过这次交易
            if (abs(trade_volume) < min_qty[symbol]) or (abs(trade_amount) < notional[symbol]):
                fee = 0.
            else:
                # 计算手续费
                fee = np.abs(trade_amount) * fee_rate
                fees[t] += fee

                # 更新仓位
                current_position[symbol] = current_position[symbol] + trade_amount / available_capacity
                current_volume[symbol] = current_volume[symbol] + trade_volume

            # 更新PnL fee
            pnl[t] -= fee
            current_capital -= fee
    pnl = np.cumsum(pnl)
    fees = np.cumsum(fees)
    return pnl, fees


def vwap_function(df: pd.DataFrame):
    if not {"amount", "volume"}.issubset(set(df.columns)):
        raise KeyError("Column 'amount' and 'volume' not found in df.")
    return np.round(df["amount"] / df["volume"], 8)


class DefaultBacktest(object):
    def __init__(self,

                 database: ParquetDatabase,
                 futures_info: dict,
                 strategy: BaseStrategy = None,
                 start_time: int = None,
                 end_time: int = None,
                 chunk_size: int = None):
        self.strategy = strategy
        self.database = database
        self.futures_info = futures_info
        self.start_time = start_time
        self.end_time = end_time
        self.chunk_size = chunk_size

        self.pnl = None
        self.fee = None
        self.indicators = {}

    def info(self):
        # Print indicators info
        ...

    def execute(self,
                strategy: BaseStrategy,
                start_time: int = None,
                end_time: int = None,
                chunk_size: int = None,
                initial_capital: float = 1e5,  # USDT
                fee_rate: float = 5e-4,  # TAKER Fee Rate
                ):
        self.strategy = strategy if strategy else self.strategy
        start_time = start_time if start_time else self.start_time
        end_time = end_time if end_time else self.end_time
        chunk_size = chunk_size if chunk_size else self.chunk_size
        symbols = self.strategy.data_symbols

        data = self.database.query_data(data_type=self.strategy.data_type,
                                        interval=self.strategy.data_interval,
                                        symbol=symbols,
                                        start_time=start_time,
                                        end_time=end_time)
        positions = self.strategy.execute(data)
        positions = positions[symbols]
        # positions: columns=symbols, index=time

        prices = pd.DataFrame({
            'price': data["amount"] / data["volume"],
            'timestamp': data['timestamp'],
            'symbol': data['symbol']
        })
        prices = prices.pivot_table(values='price', index='timestamp', columns='symbol', aggfunc='first')
        prices = prices[symbols]

        min_qty = np.asarray([self.futures_info[s]['filters']['LOT_SIZE']['minQty'] for s in symbols])
        step_size = np.asarray([self.futures_info[s]['filters']['LOT_SIZE']['stepSize'] for s in symbols])
        notional = np.asarray([self.futures_info[s]['filters']['MIN_NOTIONAL']['notional'] for s in symbols])

        self.pnl, self.fee = calculate_pnl_and_fee(positions=positions.values,
                                                   prices=prices.values,
                                                   initial_capital=initial_capital,
                                                   min_qty=min_qty, step_size=step_size, notional=notional,
                                                   fee_rate=fee_rate)

    def plot(self):
        import matplotlib.pyplot as plt
        if self.pnl and self.fee:
            plt.plot(self.pnl, "PnL")
            plt.plot(self.fee, "Fee")
            plt.show()
        else:
            print("No Pnl and fee curve to plot.")

    def __call__(self,
                 strategy: BaseStrategy,
                 start_time: int = None,
                 end_time: int = None,
                 chunk_size: int = None,
                 initial_capital: float = 1e5,  # USDT
                 fee_rate: float = 5e-4,  # TAKER Fee Rate
                 ):
        return self.execute(initial_capital=initial_capital,
                            start_time=start_time,
                            end_time=end_time,
                            chunk_size=chunk_size,
                            fee_rate=fee_rate,
                            strategy=strategy)

