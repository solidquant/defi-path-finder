import numpy as np
import pandas as pd

from defi_path_finder.collector import Collector


class Preprocessor:
    """
    Preprocesses raw form of indexed data collected by Collector
    """

    # encodes exchanges into integer values
    EXCHANGES = {
        'sushiswap_v2': 0,
        'meshswap': 1
    }

    # keeps basic information about tokens (address, symbol, decimals)
    TOKENS = {}

    # keeps contract address of pools
    POOLS = {}

    def __init__(self):
        self.collector = Collector()

        self._token_id = 0

    def polygon_sushiswap_v2_pools(self, pool_cnt: int = 500):
        pools = self.collector.get_polygon_sushiswap_v2_pools(pool_cnt)

        for p in pools:
            token0 = p['token0'].copy()
            token1 = p['token1'].copy()

            token0_id = token0.pop('id')
            token1_id = token1.pop('id')

            if token0_id not in self.TOKENS:
                self.TOKENS[token0_id] = {'id': self._token_id, **token0}
                self._token_id += 1

            if token1_id not in self.TOKENS:
                self.TOKENS[token1_id] = {'id': self._token_id, **token1}
                self._token_id += 1

        mapped = []

        for p in pools:
            t0 = self.TOKENS[p['token0']['id']]['id']
            t1 = self.TOKENS[p['token1']['id']]['id']
            pool_id = f'{t0}_{t1}_{self.EXCHANGES["sushiswap_v2"]}'
            self.POOLS[pool_id] = p['id']

            mapped.append({
                'token0': t0,
                'token1': t1,
                'exchange': self.EXCHANGES['sushiswap_v2'],
                'reserve0': float(p['reserve0']),
                'reserve1': float(p['reserve1'])
            })

        pools_df = pd.DataFrame(mapped)
        return pools_df

    def polygon_meshswap_v2_pools(self):
        tokens = self.collector.get_polygon_meshswap_tokens()
        tokens = {d['address']: {'symbol': d['symbol'], 'decimals': d['decimal']} for d in tokens}

        pools = self.collector.get_polygon_meshswap_pools()

        mapped = []

        for p in pools:
            token0 = p['token0']
            token1 = p['token1']

            if token0 not in self.TOKENS:
                token_info = tokens[token0]
                self.TOKENS[token0] = {'id': self._token_id, **token_info}
                self._token_id += 1

            if token1 not in self.TOKENS:
                token_info = tokens[token1]
                self.TOKENS[token1] = {'id': self._token_id, **token_info}
                self._token_id += 1

            t0 = self.TOKENS[token0]['id']
            t1 = self.TOKENS[token1]['id']
            pool_id = f'{t0}_{t1}_{self.EXCHANGES["meshswap"]}'
            self.POOLS[pool_id] = p['exchange_address']

            mapped.append({
                'token0': t0,
                'token1': t1,
                'exchange': self.EXCHANGES['meshswap'],
                'reserve0': float(p['amount0']),
                'reserve1': float(p['amount1'])
            })

        pools_df = pd.DataFrame(mapped)
        return pools_df

    def load_data(self, pool_cnt: int = 500):
        # 1. Collect raw indexed data
        sushi = self.polygon_sushiswap_v2_pools(pool_cnt)
        mesh = self.polygon_meshswap_v2_pools()

        # 2. Create a numpy array by mapping token, exchange data to integers
        _arr = []

        for pools_df in [sushi, mesh]:
            pools = np.zeros((len(pools_df), 3), dtype=int)
            pools[:] = pools_df[['token0', 'token1', 'exchange']].values
            _arr.append(pools)

        pools_array = np.concatenate(_arr, axis=0)

        # 3. Create a numpy array containing reserves data for all existing pools
        reserves_array = np.zeros((
            len(self.TOKENS),
            len(self.TOKENS),
            len(self.EXCHANGES),
            2
        ), dtype=float)

        for pools_df in [sushi, mesh]:
            for _, row in pools_df.iterrows():
                t0 = int(row['token0'])
                t1 = int(row['token1'])
                e = int(row['exchange'])
                r0 = row['reserve0']
                r1 = row['reserve1']

                reserves_array[t0, t1, e, :] = [r0, r1]

        return pools_array, reserves_array


if __name__ == '__main__':
    p = Preprocessor()
    pools, reserves = p.load_data()

    print(pools)