import math
import time
import requests
import pandas as pd


class Collector:
    """
    Collector indexed blockchain data from subgraphs and protocol specific endpoints
    """

    def __init__(self):
        self.URLS = {
            'polygon': {
                'sushiswap_v2': 'https://api.thegraph.com/subgraphs/name/sushiswap/matic-exchange',
                'meshswap': 'https://ss.meshswap.fi/stat/recentPoolInfo.min.json'
            }
        }

    def get_polygon_sushiswap_v2_pools(self, pool_cnt: int = 500):
        return self._get_pools('polygon', 'sushiswap_v2', pool_cnt)

    def get_polygon_meshswap_pools(self):
        return self._get_pools('polygon', 'meshswap', 0)

    def get_polygon_meshswap_tokens(self):
        tokens = requests.get('https://ss.meshswap.fi/stat/tokenInfo.min.json?t=1686709443064').json()
        tokens = pd.DataFrame(tokens)
        tokens.rename(columns=tokens.iloc[0], inplace=True)
        tokens.drop(tokens.index[0], inplace=True)
        return list(tokens.T.to_dict().values())

    def _get_pools(self, chain: str, protocol: str, pool_cnt: int):
        url = self.URLS[chain.lower()][protocol.lower()]

        if 'v2' in protocol:
            return self._request_v2_pools(url, pool_cnt)
        else:
            return self._request_special_pools(url)

    def _request_v2_pools(self, url: str, pool_cnt: int):
        query = """
        query pairs($skip: Int, $first: Int) {
            pairs(skip: $skip, first: $first, orderBy: reserveUSD, orderDirection: desc) {
                id
                token0 {
                    id
                    symbol
                    decimals
                }
                token1 {
                    id
                    symbol
                    decimals
                }
                reserve0
                reserve1
            }
        }
        """
        return self._request(url, query, pool_cnt, 2)

    def _request(self, url: str, query: str, pool_cnt: int, version: int):
        max_req = 500  # max request per request (get 500 pools per call)
        pools = []
        loop_cnt = math.ceil(pool_cnt / max_req)

        for i in range(loop_cnt):
            vars = {
                'skip': max_req * i,
                'first': max_req
            }
            res = requests.post(url, json={'query': query, 'variables': vars})
            data = res.json()
            if version == 2:
                pools.extend(data['data']['pairs'])
            print(f'{url} - skip: {vars["skip"]} / first: {vars["first"]}')
            time.sleep(1)

        return pools

    def _request_special_pools(self, url: str):
        res = requests.get(url)
        data = res.json()
        pools = pd.DataFrame(data['recentPool'])
        pools.rename(columns=pools.iloc[0], inplace=True)
        pools.drop(pools.index[0], inplace=True)
        return list(pools.T.to_dict().values())


if __name__ == '__main__':
    c = Collector()

    sushi_pools = c.get_polygon_sushiswap_v2_pools()
    mesh_pools = c.get_polygon_meshswap_pools()

    print(sushi_pools)
    print(mesh_pools)
