#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SYNOPSIS

    bench [-b,--base] [-h,--help] [-k,--rank] [-r,--ratio] [-s,--size]
          [-t,--threshold]

DESCRIPTION

    Benchmark the RecommendationBolt class from ../storm/src/py/resources.

    -b int, --base int
        Configure the size of the user base to influence the underlying SVD.
        Defaults to 2500.

    -h, --help
        Show this message.

    -k int, --rank int
        Configure the rank of the matrix approximation. Defaults to 100.

    -r float, --ratio float
        Configure the how many percent of the testing data to use for
        verification. Defaults to 0.5.

    -s int, --size int
        Configure how many users to generate recommendations for. The actual
        size may vary depending on threshold setting. Defaults to 5000.

    -t int, --threshold int
        Set the minimum ammount of observations must be on record for a user
        to be considered for testing. Defaults to 5.

AUTHOR

    Nicolas Drebenstedt <nicolas.drebenstedt@zeit.de>

LICENSE

    This script is BSD licenced, see LICENSE file for more info.

VERSION

    0.1
"""

import numpy as np
import optparse
import os
import sys
import time
import traceback


def main(base, rank, ratio, size, threshold):
    # Supress floating-point error warnings.
    np.seterr(all='ignore')

    # Configure mock recommendation bolt.
    script_path = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(script_path + '/../storm/src/py/resources')
    from recommendation import RecommendationBolt

    rb = RecommendationBolt()
    conf = {
        'zeit.recommend.svd.base': base,
        'zeit.recommend.svd.rank': rank
        }
    rb.initialize(conf, None)

    # Generate test user base with a minimum observation count of 'threshold'.
    goal = dict(
        rb.generate_seed(from_=base + 10000, size=size, threshold=threshold)
        )

    # Omit observations from user base so they can be predicted.
    test = goal.copy()
    for user in test:
        limit = int(len(test[user]) * ratio)
        test[user] = list(test[user])[:-limit]

    # Generate recommendations for incomplete test dict.
    prediction = dict()
    t0 = time.time()
    for user, events in test.items():
        vector = np.array(rb.expand({user: events}).next())
        prediction[user] = list()
        for col, value in rb.recommend(vector, size=0):
            prediction[user].append(rb.cols[col])
    execution_time = (time.time() - t0) / len(test)

    # Expand goal and prediction dicts to matrices.
    goal_matrix = np.array(list(rb.expand(goal)))
    prediction_matrix = np.array(list(rb.expand(prediction)))

    # Calculate mean absolute error.
    aggregate = 0.0
    for i in range(goal_matrix.shape[0]):
        for j in range(goal_matrix.shape[1]):
            aggregate += abs(prediction_matrix[i, j] - goal_matrix[i, j])
    mae = aggregate / np.multiply(*goal_matrix.shape)

    # Calculate average recall, precisions and f1 metrics.
    precision_aggregate = 0.0
    recall_aggregate = 0.0
    f1_aggregate = 0.0
    top_n_aggregate = 0.0
    len_goal = len(goal)

    for user, events in goal.items():
        hits = set(events).intersection(set(prediction[user]))
        
        recall = float(len(hits)) / len_goal
        recall_aggregate += recall

        if len(prediction[user]):
            precision = float(len(hits)) / len(prediction[user])
        else:
            precision = 0
        precision_aggregate += precision

        if (recall + precision):
            f1 = (2 * recall * precision) / (recall + precision)
        else:
            f1 = 0
        f1_aggregate += f1

        top_n_aggregate += len(prediction[user])

    recall = recall_aggregate / len_goal
    precision = precision_aggregate / len_goal
    f1 = f1_aggregate / len_goal
    top_n = top_n_aggregate / len_goal

    header = lambda h: ' %s '.rjust(16, '-').ljust(24, '-') % h

    print header('Options')
    print 'Base:\t\t', len(rb.rows)
    print 'Rank:\t\t', options.rank
    print 'Ratio:\t\t', options.ratio
    print 'Size:\t\t', len(goal)
    print 'Threshold:\t', options.threshold
    print header('Averages')
    print 'Seconds:\t%.8f' % execution_time
    print 'MAE:\t\t%.8f' % mae
    print 'Recall:\t\t%.8f' % recall
    print 'Precision:\t%.8f' % precision
    print 'F1 Metric:\t%.8f' % f1
    print 'Top N:\t\t%.8f' % top_n

if __name__ == '__main__':
    try:
        parser = optparse.OptionParser(
            formatter=optparse.TitledHelpFormatter(),
            usage=globals()['__doc__'],
            version='0.1'
            )
        parser.add_option(
            '-b',
            '--base',
            default=2500,
            help='size of original user base',
            type='int'
            )
        parser.add_option(
            '-k',
            '--rank',
            default=100,
            help='rank of matrix approximation',
            type='int'
            )
        parser.add_option(
            '-r',
            '--ratio',
            default=0.5,
            help='ratio of testing to verification data',
            type='float'
            )
        parser.add_option(
            '-s',
            '--size',
            default=5000,
            help='size of test user base',
            type='int'
            )
        parser.add_option(
            '-t',
            '--threshold',
            default=5,
            help='minimum observations per test user',
            type='int'
            )
        (options, args) = parser.parse_args()
        main(
            options.base,
            options.rank,
            options.ratio,
            options.size,
            options.threshold
            )
    except SystemExit, e:
        raise e
    except UserWarning, e:
        print str(e)
        os._exit(1)
    except Exception, e:
        print str(e)
        traceback.print_exc()
        os._exit(1)
