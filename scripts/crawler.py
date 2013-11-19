#!/usr/bin/env python
"""
SYNOPSIS

    crawler [-t,--threshold] [-h,--help] [-v,--verbose] [--version] api_key

DESCRIPTION

    This ZEIT ONLINE Content API crawler can generate dummy user data by
    pretending authors are users and associated articles were read by them.
    To use this script you need to signup for an API key at:
    http://developer.zeit.de

EXIT STATUS

    0     Dummy data was successfully downloaded.
    1     An error occured.

AUTHOR

    Nicolas Drebenstedt <nicolas.drebenstedt@zeit.de>

LICENSE

    This script is BSD licenced, see LICENSE file for more info.

VERSION

    0.1
"""

import csv
import json
import optparse
import os
import sys
import time
import traceback
import urllib


class CSVWriter(object):

    def __init__(self, name):
        self._csv_file = open(name + '.csv', 'a')
        self._writer = csv.writer(self._csv_file, delimiter=',')

    def __call__(self, key, pairs):
        for id_, val in pairs:
            self._writer.writerow([key, id_, val])


def api_fetch(endpoint, **kwargs):
    base = 'http://api.zeit.de/'
    endpoint = base + endpoint if base not in endpoint else endpoint
    url = '%s?%s' % (endpoint, urllib.urlencode(kwargs))
    try:
        return json.loads(urllib.urlopen(url).read())
    except:
        return dict(matches=[])


def write_ratings(authors):
    global stats

    for author in authors:
        if isinstance(author, int):
            continue
        if authors[authors.index(author) - 1] < options.threshold:
            # Only select authors that reach the threshold.
            stats['Threshold not reached'] += 1
            continue
        if len(author) < 5 or len(author) > 50:
            # Simply sanity checks on the name length.
            stats['Sanity check failed'] += 1
            continue
        if ',' in author or author.count(' ') > 5:
            # Simply sanity checks on unwanted characters.
            stats['Sanity check failed'] += 1
            continue
        try:
            # Ignore unicode author names for now.
            author.encode('ascii')
        except:
            stats['Unparsable unicode'] += 1
            continue

        result = api_fetch('author', api_key=args[0], limit=1, q=author)

        if not len(result['matches']):
            stats['Author id not found'] += 1
            continue

        id_ = result['matches'][0]['id']

        result = api_fetch('author/' + id_, api_key=args[0])

        if not 'matches' in result:
            stats['No articles found'] += 1
            continue

        write = CSVWriter('ratings')

        for uri in list([m['uri'] for m in result['matches']]):
            match = api_fetch(uri, api_key=args[0])
            pairs = [(r['uri'].split('/')[-1], 1) for r in match['relations']]
            pairs += [(match['uuid'], 2)]
            write(id_, pairs)

    return stats


def main():
    global options, args, stats

    stats = {
        'Threshold not reached': 0,
        'Sanity check failed': 0,
        'Unparsable unicode': 0,
        'Author id not found': 0,
        'No articles found': 0
        }

    result = api_fetch('content', api_key=args[0], facet_field='author')
    facets = result['facets']['author']
    l, s = len(facets), 500
    chunks = zip(range(0, l, s), range(s - 1, l, s)) + [(l - (l % s), l - 1)]

    for lower, upper in chunks:
        write_ratings(facets[lower:upper])
        sys.stdout.write('Processed %s user records.\r' % (upper + 1))
        sys.stdout.flush()

    print 'Done. Processed %s user records.' % l


if __name__ == '__main__':
    try:
        start_time = time.time()
        parser = optparse.OptionParser(
            formatter=optparse.TitledHelpFormatter(),
            usage=globals()['__doc__'],
            version='0.1'
            )
        parser.add_option(
            '-v',
            '--verbose',
            default=False,
            help='verbose output'
            )
        parser.add_option(
            '-t',
            '--threshold',
            type='int',
            default=5,
            help='minimum article count'
            )
        (options, args) = parser.parse_args()
        if len(args) < 1:
            parser.error('Missing argument: api_key.')
        if options.verbose:
            print time.asctime()
        main()
        if options.verbose:
            print time.asctime()
            print 'Total time in minutes: ', (time.time() - start_time) / 60.0
            print reduce(lambda i, x: i + '%s: %s, ' % x, stats.items(), '')
        sys.exit(0)
    except ImportError, e:
        print str(e)
        os._exit(1)
    except KeyboardInterrupt, e:
        raise e
    except SystemExit, e:
        raise e
    except UserWarning, e:
        print str(e)
        os._exit(1)
    except Exception, e:
        print str(e)
        traceback.print_exc()
        os._exit(1)
