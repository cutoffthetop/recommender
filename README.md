# zeit.recommend

This guide describes how to setup the Zeit Recommend service.

Currently two machines are needed:

* Varnish caching server
* RabbitMQ queuing server

-------------------------------------------------------------------------------

(Most commands must be run as root.)

## Varnish

1. install `openbsd-inetd` or equivalent
	apt-get install openbsd-inetd
2. copy the contents from `varnish/etc/inetd.conf` to `/etc/inetd.conf`
3. Reload the inetd config
	sudo /etc/init.d/openbsd-inetd restart
4. Allow TCP traffic on port 3000

## Rabbit

1. Add a new user for logstash
	useradd -m logstash
3. Copy the logstash config file
	mkdir /etc/logstash && cp logstash/etc/logstash/agent.conf /etc/logstash/
4. Paste the varnish host name into this line in /etc/logstash/agent.conf
	host => "{INSERT HOST URL HERE}"
3. Download logstash jar file
	curl https://download.elasticsearch.org/logstash/logstash/logstash-1.2.2-flatjar.jar /home/logstash/logstash.jar
4. Add the following line to your /etc/apt/sources.list
	deb http://www.rabbitmq.com/debian/ testing main
5. Trust the rabbitmq public key
	wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
	apt-key add rabbitmq-signing-key-public.asc
6. Update package index
	apt-get update
7. Install required packages
	apt-get install openjdk-6-jre python-dev python-setuptools git rabbitmq-server upstart
8. Ensure rabbit is running
	rabbitmqctl status
2. Copy the logstash upstart config
	cp logstash/etc/init/logstash-agent.conf /etc/init/
1. Launch logstash using upstart
	initctl start logstash-agent
9. Install pika
	easy_install pika

## Consumer

1. Run the simple python message consumer
	python scripts/consumer zr_spout logstash
