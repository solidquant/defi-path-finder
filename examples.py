import pickle

from defi_path_finder import Preprocessor
from defi_path_finder import make_triangular_paths


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

    print(paths)