import numpy as np
from tqdm import tqdm
import multiprocessing
from itertools import combinations, permutations, product

multiprocessing.freeze_support()


def make_triangular_paths(data: np.array,
                          include_tokens: list = [],
                          process_cnt: int = 5) -> (dict, np.array):

    pairs = make_3_pairs(data, include_tokens, process_cnt)
    paths = make_paths_array(pairs)
    return pairs, paths


def make_3_pairs(data: np.array,
                 include_tokens: list,
                 process_cnt: int = 5) -> dict:

    """
    :param data: is an array type from Preprocessor.load_data() (pools_array)
    :param include_tokens: can filter out tokens. if you input [0] and 0 is WMATIC, you make sure
                           to create 3 pairs that have WMATIC in it
    :param process_cnt: the number of cpu's. To be used in multiprocessing
    :return: dict type value that is used in make_paths_array
    """
    if len(include_tokens) > 0:
        # filter out the data beforehand to optimize running time
        data = data[np.any(np.isin(data[:, :2], include_tokens), axis=1)]

    tokens = np.unique(data[:, :2])
    combinations_list = list(combinations(tokens, 3))

    process_inputs = _make_proceess_inputs(data,
                                           include_tokens,
                                           combinations_list,
                                           process_cnt)

    mp = multiprocessing.Pool(processes=process_cnt)

    pairs = {}

    for result in mp.starmap(_make_3_pairs_process, process_inputs):
        pairs = {**result, **pairs}

    return pairs


def make_paths_array(pairs: dict) -> np.array:
    """
    :param pairs: should be dict from running make_3_pairs
    :return: np.array value of all possible triangular paths
    """
    all_paths = []

    i = 0

    for key, filtered in pairs.items():
        tokens = np.array(key)
        path_combos = np.array(list(permutations(tokens)))

        pathify = lambda x: np.vstack((x, np.roll(x, -1))).T
        possible_paths = [pathify(c) for c in path_combos]

        match_pools = lambda x, match: x[(x[:, -2:] == match).all(axis=1)]

        for p in possible_paths:
            matched_pools = [match_pools(filtered, token_in_out) for token_in_out in p]
            path = [np.array(path) for path in product(*matched_pools)]
            all_paths.extend(path)
            # print(f'({i} / {len(pairs)}) {p.reshape(-1)}: {len(path)} added')

        i += 1

    return np.array(all_paths)


def _make_proceess_inputs(data: np.array,
                          include_tokens: list,
                          combinations_list: list,
                          process_cnt: int):

    n = len(combinations_list)
    part_size = n // process_cnt
    extra = n % process_cnt

    inputs = []
    start = 0

    for i in range(process_cnt):
        if i < extra:
            end = start + part_size + 1
        else:
            end = start + part_size

        inputs.append((data, include_tokens, combinations_list[start:end]))
        start = end

    return inputs


def _make_3_pairs_process(data: np.array,
                          include_tokens: list,
                          combinations_list: list):

    pairs = {}

    for c in tqdm(combinations_list):
        if not set(include_tokens).issubset(set(c)):
            continue

        # 1. filter data where token0, token1 are both in the combo set
        #    a combo set would look like: (0, 1, 2)
        filtered_array = data[np.all(np.isin(data[:, :2], c), axis=1)]
        _f = filtered_array[:, :2]

        # filtered_array should contain 3 unique tokens and at least 3 unique pools for triangular arbitrage
        if not np.unique(_f).shape[0] == 3 or not np.unique(_f, axis=0).shape[0] >= 3:
            continue

        # 2: create a new array with token_in, token_out info
        # the columns for full_array will be: token0, token1, exchange, token_in, token_out
        full_array = np.zeros((filtered_array.shape[0] * 2, 5), dtype=int)
        full_array[:, :3] = np.concatenate([filtered_array, filtered_array])
        full_array[:, 3:] = np.concatenate([_f, np.flip(_f, axis=1)])

        pairs[c] = full_array

    return pairs
