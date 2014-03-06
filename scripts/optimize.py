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
import numpy as np
import optparse
import os
import sys


def predict(params):
    neighbors, rank = params

    script_path = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(script_path + '/../storm/src/py/resources')
    from recommendation import RecommendationBolt
    from morelikethis import MorelikethisBolt

    conf = {
        'zeit.recommend.svd.base': 10000,
        'zeit.recommend.svd.rank': rank,
        'zeit.recommend.elasticsearch.host': 'localhost',
        'zeit.recommend.zonapi.host': '217.13.68.229'
        }
    rb = RecommendationBolt()
    rb.initialize(conf, None)
    mb = MorelikethisBolt()
    mb.initialize(conf, None)

    goal = dict(
        rb.generate_seed(size=1000, threshold=0.25)
        ).items()

    goal_matrix = np.array(list(rb.expand(g[1]) for g in goal))

    test = goal[:]
    for i in range(len(test)):
        test[i] = test[i][0], list(test[i][1])[:-int(len(test[i][1]) * 0.5)]

    mae_aggr = 0.0
    prediction_matrix = np.zeros_like(goal_matrix)
    for i in range(len(test)):
        vector = rb.expand(test[i][1])
        prediction_matrix[i, :] = rb.predict(vector, neighbors=neighbors)
    for i in range(goal_matrix.shape[0]):
        for j in range(goal_matrix.shape[1]):
            mae_aggr += abs(prediction_matrix[i, j] - goal_matrix[i, j])
    mae = mae_aggr / np.multiply(*goal_matrix.shape)

    xval_aggr = 1.0
    for i in range(len(test)):
        val = mb.recommend(test[i][1], top_n=100)
        diff = goal[i][1].difference(test[i][1])
        xval_aggr += len(diff.intersection(val))
    xval = xval_aggr / float(len(test))

    f1_aggr = 0.0
    for user, paths in test:
        vector = rb.expand(paths)
        docs = rb.recommend(vector, proximity=0.5, neighbors=neighbors)
        intersection = float(len(set(rb.cols).intersection(docs)))
        f1_aggr += intersection / (len(rb.cols) + len(docs))
    f1 = f1_aggr / float(len(test))

    line = ['%.9f' % i for i in (neighbors, rank, mae, xval, f1)]
    open('/tmp/opt.csv', 'a').write('\n' + '\t'.join(line))


def tolerant_predict(params):
    try:
        return predict(params)
    except Exception, e:
        line = '\n' + '\t'.join(['%.9f' % i for i in params] + [e.message])
        open('/tmp/opt.csv', 'a').write(line)


if __name__ == '__main__':
    parser = optparse.OptionParser(
        formatter=optparse.TitledHelpFormatter(),
        usage=globals()['__doc__'],
        version='0.1'
        )
    parser.add_option(
        '-t',
        '--tolerant',
        action='store_true',
        help='tolerate runtime errors'
        )
    (options, args) = parser.parse_args()

    line = '\t'.join(['neighbors', 'rank', 'mae', 'xval', 'f1'])
    open('/tmp/opt.csv', 'a').write(line)

    prefix = 'tolerant_' if options.tolerant else ''
    func = globals().get('%spredict' % prefix)
    steps = [10, 59, 108, 157, 206, 255, 304, 353, 402, 451, 500]
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    promise = pool.map_async(func, list(itertools.product(steps, steps)), 16)
    try:
        promise.wait()
    except KeyboardInterrupt:
        pool.terminate()
