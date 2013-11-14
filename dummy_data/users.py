#!/usr/bin/env python
"""
SYNOPSIS

    users [-t,--threshold] [-h,--help] [-v,--verbose] [--version] api_key

DESCRIPTION

    TODO This describes how to use this script. This docstring
    will be printed by the script if there is an error or
    if the user requests help (-h or --help).

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
import random
import sys
import time
import traceback
import urllib


def api_fetch(endpoint, **kwargs):
    base = 'http://api.zeit.de/'
    endpoint = base + endpoint if base not in endpoint else endpoint
    url = '%s?%s' % (endpoint, urllib.urlencode(kwargs))
    try:
        return json.loads(urllib.urlopen(url).read())
    except:
        return dict(matches=[])


def str_to_int(string):
    return int(''.join(format(ord(x), 'b') for x in string), 2)


def write_records(authors):
    global stats

    valid_authors = []

    with open('ratings.csv', 'a') as csvfile:
        rating_writer = csv.writer(csvfile, delimiter=',')

        for author in authors:
            if isinstance(author, int):
                continue
            if authors[authors.index(author) - 1] < options.threshold:
                # Only select authors that reach the threshold.
                stats['Threshold not reached'] += 1
                continue
            if len(author) < 5 or len(author) > 50 or author.count(' ') > 5:
                # Simply sanity checks on the author's name.
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

            author_id = str_to_int(id_)
            valid_authors.append(author_id)

            for uri in list([m['uri'] for m in result['matches']]):
                article = api_fetch(uri, api_key=args[0])
                article_id = str_to_int(article['uuid'])
                rating_writer.writerow((author_id, article_id, 2))
                for related in article['relations']:
                    relation_id = str_to_int(related['uri'].split('/')[-1])
                    rating_writer.writerow((author_id, relation_id, 1))

    with open('user.csv', 'a') as csvfile:
        if len(valid_authors):
            user_writer = csv.writer(csvfile, delimiter=',')
            user_writer.writerow((random.choice(valid_authors),))

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
        write_records(facets[lower:upper])
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
