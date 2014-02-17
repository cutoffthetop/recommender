#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SYNOPSIS

    optimize

DESCRIPTION

    Optimize parameters of RecommendationBolt class using annealing.

    -f, --force
        Use brute force algorithm instead of annealing.

    -t, --tolerant
        Tolerate runtime errors in optimization function.

AUTHOR

    Nicolas Drebenstedt <nicolas.drebenstedt@zeit.de>

LICENSE

    This script is BSD licenced, see LICENSE file for more info.

VERSION

    0.1
"""

import itertools
import multiprocessing
import optparse
import os
import sys

import numpy as np
from scipy.optimize import anneal


def predict(params):
    params = getattr(params, 'tolist', lambda: params)()
    base, neighbors, rank, ratio, threshold = params

    base = abs(int(base))
    neighbors = abs(int(neighbors))
    rank = abs(min(int(rank), base))
    ratio = abs(float(ratio) / 100)
    threshold = abs(float(threshold) / 100)

    script_path = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(script_path + '/../storm/src/py/resources')
    from recommendation import RecommendationBolt

    rb = RecommendationBolt()
    conf = {
        'zeit.recommend.svd.base': base,
        'zeit.recommend.svd.rank': rank,
        'zeit.recommend.elasticsearch.host': '217.13.68.236'
        }
    rb.initialize(conf, None)

    goal = dict(
        rb.generate_seed(from_=base, size=100, threshold=threshold)
        ).items()

    goal_matrix = np.array(list(rb.expand(g[1]) for g in goal))

    test = goal[:]
    for i in range(len(test)):
        test[i] = test[i][0], list(test[i][1])[:-int(len(test[i][1]) * ratio)]

    prediction_matrix = np.zeros_like(goal_matrix)

    for i in range(len(test)):
        vector = rb.expand(test[i][1])
        prediction_matrix[i, :] = rb.predict(vector, neighbors=neighbors)

    error_aggregate = 0.0
    for i in range(goal_matrix.shape[0]):
        for j in range(goal_matrix.shape[1]):
            error_aggregate += abs(prediction_matrix[i, j] - goal_matrix[i, j])

    mae = error_aggregate / np.multiply(*goal_matrix.shape)
    output = [len(goal), neighbors, rank, ratio, threshold, mae]
    print '\t'.join(['%.4f' % i for i in output])
    return mae


def tolerant_predict(params):
    try:
        return predict(params)
    except Exception, e:
        return '\t'.join(['%.4f' % i for i in params] + [e.message])


def brute_optimize(func, steps=10, **kwargs):
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    bounds = zip(kwargs['lower'], kwargs['upper'])
    opt = [(range(l, u, (u - l) / steps)[:steps - 1] + [u]) for l, u in bounds]
    return pool.map(func, list(itertools.product(*opt)))


def anneal_optimize(func, x0=(500, 10, 100, 50, 25), **kwargs):
    return anneal(func, x0, **kwargs)


if __name__ == '__main__':
    print '\t'.join(['base', 'neighbors', 'rank', 'ratio', 'threshold', 'mae'])

    parser = optparse.OptionParser(
        formatter=optparse.TitledHelpFormatter(),
        usage=globals()['__doc__'],
        version='0.1'
        )
    parser.add_option(
        '-f',
        '--force',
        action='store_true',
        help='brute force algorithm'
        )
    parser.add_option(
        '-t',
        '--tolerant',
        action='store_true',
        help='tolerate runtime errors'
        )
    (options, args) = parser.parse_args()

    gf = globals().get
    algo = gf('%s_optimize' % ('brute' if options.force else 'anneal'))
    func = gf('%spredict' % ('tolerant_' if options.tolerant else ''))

    print algo(
        func,
        lower=(100, 1, 5, 10, 5),
        upper=(7500, 400, 500, 90, 65)
        )
