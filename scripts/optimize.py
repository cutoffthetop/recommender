#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SYNOPSIS

    optimize

DESCRIPTION

    Optimize parameters of RecommendationBolt class within given bounds.
    No parameters accepted.

AUTHOR

    Nicolas Drebenstedt <nicolas.drebenstedt@zeit.de>

LICENSE

    This script is BSD licenced, see LICENSE file for more info.

VERSION

    0.1
"""

import sys
import os
import numpy as np
from scipy.optimize import anneal


def predict(params):
    base, neighbors, rank, ratio, threshold = params.tolist()

    base = abs(int(base))
    neighbors = abs(int(neighbors))
    rank = abs(min(int(rank), base))
    ratio = abs(float(ratio))
    threshold = abs(float(threshold))

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

    if not len(goal):
        print '\t'.join(['%.2f' % i for i in params.tolist()] + ['1.0'])
        return 1.0

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
    print '\t'.join(['%.2f' % i for i in params.tolist()] + [str(mae)])
    return mae


if __name__ == '__main__':
    print anneal(
        predict,
        (500.0, 10.0, 100.0, 0.5, 0.25),
        full_output=1,
        dwell=25,
        disp=1,
        lower=(100.0, 1.0, 5.0, 0.1, 0.05),
        upper=(7500.0, 400.0, 500.0, 0.9, 0.65)
        )
