#!/usr/bin/env python
"""
SYNOPSIS

    consumer [-n,--name] [-t, --type] [-h,--help] exchange routing_key

DESCRIPTION

    Simple RabbitMQ consumer that prints incoming messages to stdout.

    The following options are available:

    -n host_name, --name host_name
        Configure the host name of the RabbitMQ server, default is: localhost

    -t exchange_type, --type exchange_type
        Configure the type of the exchange, one of: fanout, direct, topic
        Default is: direct

AUTHOR

    Nicolas Drebenstedt <nicolas.drebenstedt@zeit.de>

LICENSE

    This script is BSD licenced, see LICENSE file for more info.

VERSION

    0.1
"""

import optparse
import os
import pika
import traceback


def callback(ch, method, properties, body):
    print '[x] %r:%r' % (method.routing_key, body,)


def main():
    parameters = pika.ConnectionParameters(host=options.name, port=5672)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange=args[0], type=options.type)

    queue = channel.queue_declare(exclusive=True).method.queue

    channel.queue_bind(exchange=args[0], queue=queue, routing_key=args[1])

    print '\x1b[1A\x1b[2K[*] Waiting for logs. To exit press CTRL+C'

    channel.basic_consume(callback, queue=queue, no_ack=True)

    channel.start_consuming()


if __name__ == '__main__':
    try:
        parser = optparse.OptionParser(
            formatter=optparse.TitledHelpFormatter(),
            usage=globals()['__doc__'],
            version='0.1'
            )
        parser.add_option(
            '-n',
            '--name',
            default='localhost',
            help='host name of the RabbitMQ server'
            )
        parser.add_option(
            '-t',
            '--type',
            default='direct',
            help='type of the RabbitMQ exchange'
            )
        (options, args) = parser.parse_args()
        if len(args) < 2:
            parser.error('Missing arguments: exchange, routing_key')
        elif len(args) < 1:
            parser.error('Missing argument: routing_key')
        main()
    except KeyboardInterrupt, e:
        print '\r[*] Shutting down message consumer.'
        os._exit(0)
    except SystemExit, e:
        raise e
    except UserWarning, e:
        print str(e)
        os._exit(1)
    except Exception, e:
        print str(e)
        traceback.print_exc()
        os._exit(1)
