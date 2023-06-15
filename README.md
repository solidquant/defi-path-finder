# DeFi Arbitrage Path Finder

A simple arbitrage path finder for multiple DEXs.

Two way arbitrage paths are rather simple to find. However, n-way paths get complicated real quick.

This project is an attempt to map triangular paths using 2 DEXs.
The concepts used here can easily extend over to 4-way, 5-way, n-way paths.

The sample exchanges used here are:
- Polygon Sushiswap V2,
- Polygon Meshswap

Sushiswap V2 data is retrieved through a subgraph endpoint.
And Meshswap data is retrieved through their specific API endpoint.


### Usage:

Install dependencies:
```bash
pip install -r requirements.txt
```

Run the sample code in examples.py:

```python
import pickle

from preprocessor import Preprocessor
from utils import make_triangular_paths


if __name__ == '__main__':
    p = Preprocessor()
    pools, reserves = p.load_data(500)

    pairs, paths = make_triangular_paths(pools, [], 6)

    # pickle data to load and use later
    save_data = {
        'EXCHANGES': p.EXCHANGES,
        'TOKENS': p.TOKENS,
        'POOLS': p.POOLS,
        'reserves': reserves,
        'pairs': pairs,
        'paths': paths
    }

    f = open('./paths.pkl', 'wb')
    pickle.dump(save_data, f)
    f.close()
```

The code above runs for approximately 10 minutes using 6 processes to run this code in parallel.

### TODO:

- add simulation helpers to find the most profitable arbitrage path
- extend the module to work for n-way paths as well
- add market impact simulation functions for various AMM types to accommodate for all variants of Uniswap and others