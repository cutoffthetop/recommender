# zeit.recommend

This guide describes how to setup the Zeit Recommend service.    
(Most commands must be run as root.)

Currently two machines are needed:

* Varnish caching server
* RabbitMQ queuing server

-------------------------------------------------------------------------------

## Varnish

* Install `openbsd-inetd` or equivalent

```shell
apt-get install openbsd-inetd
```

* Configure inetd for varnishncsa

```shell
cat varnish/etc/inetd.conf >> /etc/inetd.conf
```

* Reload the inetd config

```shell
sudo /etc/init.d/openbsd-inetd restart
```

* Allow TCP traffic on port 3000

## Rabbit

* Add a new user for logstash

```shell
useradd -m logstash
```

* Copy the logstash config file

```shell
mkdir /etc/logstash
cp logstash/etc/logstash/agent.conf /etc/logstash/
```

* Paste the varnish host name into this line in /etc/logstash/agent.conf

```shell
host => "{INSERT HOST URL HERE}"
```

* Download logstash jar file

```shell
curl https://download.elasticsearch.org/logstash/logstash/logstash-1.2.2-flatjar.jar \
/home/logstash/logstash.jar
```

* Add the following line to your /etc/apt/sources.list

```shell
deb http://www.rabbitmq.com/debian/ testing main
```

* Trust the rabbitmq public key

```shell
wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
apt-key add rabbitmq-signing-key-public.asc
```

* Update package index

```shell
apt-get update
```

* Install required packages

```shell
apt-get install openjdk-6-jre python-dev python-setuptools rabbitmq-server upstart
```

* Ensure rabbit is running

```shell
rabbitmqctl status
```

* Copy the logstash upstart config

```shell
cp logstash/etc/init/logstash-agent.conf /etc/init/
```

* Launch logstash using upstart

```shell
initctl start logstash-agent
```

* Install pika

```shell
easy_install pika
```

## Consumer

* Run the simple python message consumer

```shell
python scripts/consumer.py zr_spout logstash
```
