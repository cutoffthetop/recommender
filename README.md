# zeit.recommend

This guide describes how to setup the Zeit Recommend service.

Currently three machines are needed:

* Varnish caching server
* RabbitMQ queuing server
* Storm processing server

-------------------------------------------------------------------------------

## Varnish

* Install `openbsd-inetd` or equivalent

```shell
sudo apt-get install openbsd-inetd
```

* Configure inetd for varnishncsa

```shell
sudo cat varnish/etc/inetd.conf >> /etc/inetd.conf
```

* Reload the inetd config

```shell
sudo /etc/init.d/openbsd-inetd restart
```

* Allow TCP traffic on port 3000

## Rabbit

* Add a new user for logstash

```shell
sudo useradd -m logstash
```

* Copy the logstash config file

```shell
sudo mkdir /etc/logstash
sudo cp logstash/etc/logstash/agent.conf /etc/logstash/
```

* Paste the varnish host name into this line in /etc/logstash/agent.conf

```shell
host => "{INSERT HOST URL HERE}"
```

* Download logstash jar file

```shell
sudo curl https://download.elasticsearch.org/logstash/logstash/logstash-1.2.2-flatjar.jar > /home/logstash/logstash.jar
```

* Add the following line to your /etc/apt/sources.list

```shell
deb http://www.rabbitmq.com/debian/ testing main
```

* Trust the rabbitmq public key

```shell
wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
sudo apt-key add rabbitmq-signing-key-public.asc
```

* Update package index

```shell
sudo apt-get update
```

* Install required packages

```shell
sudo apt-get install openjdk-6-jre python-setuptools rabbitmq-server upstart
```

* Ensure rabbit is running

```shell
sudo rabbitmqctl status
```

* Install pika

```shell
sudo easy_install pika
```

### Ubuntu

If using ubuntu, logstash can be configured as a daemon using upstart.

* Copy the logstash upstart config

```shell
sudo cp logstash/etc/init/logstash-agent.conf /etc/init/
```

* Launch logstash using upstart

```shell
initctl start logstash-agent
```

### Debian

If using debian, logstash should be configured using update-rc.d.    
Note: Make sure your PATH includes the sbin folders.

```shell
sudo cp logstash/etc/init.d/logstash-agent /etc/init.d/
```

```shell
update-rc.d logstash-agent defaults
```

### Consumer

* Run a simple AMPQ consumer to check your connection status.

```shell
scripts/consumer.py zr_spout logstash
```

## Storm

You can run the storm topology locally or deploy it to a cluster.

### Development

To set up a development environment follow the steps below.

* Make sure you have Sun or Oracle JDK version 6 installed.
* Install the latest version of the IntelliJ IDEA IDE.
* Select `Open File`, `Import Project...` and choose the `storm` directory.
* Import from `Maven` model and make sure the option `Import Maven projects automatically` is checked.
* Now select the `zeit.recommend` project and an appropriate JDK.

### Local mode

* Make sure, you have Maven and storm installed on your machine.

* Then change to the `storm/` directory and run

```shell
mvn -f pom.xml compile exec:java -Dstorm.topology=zeit.recommend.Recommender
```

### Cluster setup

Coming soon.
