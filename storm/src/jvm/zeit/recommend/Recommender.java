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
        "zonapi",
        new PythonSpout("zonapi.py", "path"),
        1);

     builder.setBolt(
        "item",
        new PythonBolt("item.py", "path"),
        1).shuffleGrouping("zonapi");

    builder.setSpout(
        "rabbitmq",
        new PythonSpout("rabbitmq.py", "timestamp", "path", "user"),
        1);

    builder.setBolt(
        "user",
        new PythonBolt("user.py", "user", "paths"),
        1).shuffleGrouping("rabbitmq");

    builder.setBolt(
        "recommendation",
        new PythonBolt("recommendation.py", "user", "events", "recommendations"),
        1).shuffleGrouping("user");

    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxTaskParallelism(1);

    // TODO: Read config data from file.
    // conf.put("zeit.recommend.elasticsearch.host", "localhost");
    conf.put("zeit.recommend.elasticsearch.host", "217.13.68.236");
    conf.put("zeit.recommend.elasticsearch.port", 9200);
    conf.put("zeit.recommend.rabbitmq.exchange", "zr_spout");
    conf.put("zeit.recommend.rabbitmq.host", "217.13.68.236");
    conf.put("zeit.recommend.rabbitmq.key", "logstash");
    conf.put("zeit.recommend.rabbitmq.port", 5672);
    conf.put("zeit.recommend.rabbitmq.throughput", 0.1);
    conf.put("zeit.recommend.svd.base", 5000);
    conf.put("zeit.recommend.svd.rank", 250);
    conf.put("zeit.recommend.zonapi.host", "217.13.68.229");
    conf.put("zeit.recommend.zonapi.port", 8983);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("zeit-recommend", conf, builder.createTopology());

    Thread.sleep(1 * 1 * 60 * 1000);
    cluster.shutdown();
  }
}
