package zeit.recommend;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * zeit.recommend.Recommender
 *
 * This class has no description.
 *
 * Copyright: (c) 2013 by Nicolas Drebenstedt.
 * License: BSD, see LICENSE for more details.
 */

public class Recommender {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(
        "rabbitmq",
        new PythonSpout("rabbitmq.py", "timestamp", "path", "user"),
        1);

    builder.setSpout(
        "zonapi",
        new PythonSpout("zonapi.py", "path"),
        1);

    builder.setBolt(
        "observation",
        new PythonBolt("observation.py", "user", "paths"),
        1).shuffleGrouping("rabbitmq");

    builder.setBolt(
        "recommendation",
        new PythonBolt("recommendation.py", "user", "events", "recommendations"),
        1).shuffleGrouping("observation");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxTaskParallelism(1);

    // TODO: Read config data from file.
    conf.put("zeit.recommend.elasticsearch.host", "localhost");
    conf.put("zeit.recommend.elasticsearch.port", 9200);
    conf.put("zeit.recommend.rabbitmq.exchange", "zr_spout");
    conf.put("zeit.recommend.rabbitmq.host", "217.13.68.236");
    conf.put("zeit.recommend.rabbitmq.key", "logstash");
    conf.put("zeit.recommend.rabbitmq.port", 5672);
    conf.put("zeit.recommend.rabbitmq.throughput", 15);
    conf.put("zeit.recommend.svd.base", 5000);
    conf.put("zeit.recommend.svd.rank", 250);
    conf.put("zeit.recommend.zonapi.host", "217.13.68.229");
    conf.put("zeit.recommend.zonapi.port", 8983);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("zeit-recommend", conf, builder.createTopology());

    Thread.sleep(1 * 60 * 1000);
    cluster.shutdown();
  }
}
