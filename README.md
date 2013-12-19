# zeit.recommend

This guide describes how to setup the Zeit Recommend service.    
(Most commands must be run as root.)

Currently two machines are needed:

* Varnish caching server
* RabbitMQ queuing server

-------------------------------------------------------------------------------

## Varnish

1. install `openbsd-inetd` or equivalent

```shell
apt-get install openbsd-inetd
```

2. copy the contents from `varnish/etc/inetd.conf` to `/etc/inetd.conf`
3. Reload the inetd config

```shell
sudo /etc/init.d/openbsd-inetd restart
```

4. Allow TCP traffic on port 3000

## Rabbit

1. Add a new user for logstash

```shell
useradd -m logstash
```

3. Copy the logstash config file

```shell
mkdir /etc/logstash && cp logstash/etc/logstash/agent.conf /etc/logstash/
```

4. Paste the varnish host name into this line in /etc/logstash/agent.conf

```shell
host => "{INSERT HOST URL HERE}"
```

3. Download logstash jar file

```shell
curl https://download.elasticsearch.org/logstash/logstash/logstash-1.2.2-flatjar.jar /home/logstash/logstash.jar
```

4. Add the following line to your /etc/apt/sources.list

```shell
deb http://www.rabbitmq.com/debian/ testing main
```

5. Trust the rabbitmq public key

```shell
wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc && apt-key add rabbitmq-signing-key-public.asc
```

6. Update package index

```shell
apt-get update
```

7. Install required packages

```shell
apt-get install openjdk-6-jre python-dev python-setuptools git rabbitmq-server upstart
```

8. Ensure rabbit is running

```shell
rabbitmqctl status
```

2. Copy the logstash upstart config

```shell
cp logstash/etc/init/logstash-agent.conf /etc/init/
```

1. Launch logstash using upstart

```shell
initctl start logstash-agent
```

9. Install pika

```shell
easy_install pika
```

## Consumer

1. Run the simple python message consumer

```shell
python scripts/consumer zr_spout logstash
```
