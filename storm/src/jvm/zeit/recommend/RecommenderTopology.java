package zeit.recommend;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.ShellSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

public class RecommenderTopology {

  public static class RabbitMQSpout extends ShellSpout implements IRichSpout {

    public RabbitMQSpout() {
      super("python", "rabbitmq.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("timestamp", "path", "user"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return new HashMap<String, Object>();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      super.open(conf, context, collector);
    }
  }

  public static class ObservationBolt extends ShellBolt implements IRichBolt {

    public ObservationBolt() {
      super("python", "observation.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("user", "paths"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return new HashMap<String, Object>();
    }
  }

  public static class RecommendationBolt extends ShellBolt implements IRichBolt {

    public RecommendationBolt() {
      super("python", "recommendation.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("user", "paths", "recommendations"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return new HashMap<String, Object>();
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("rabbitmq", new RabbitMQSpout(), 1);
    builder.setBolt("observation", new ObservationBolt(), 1).shuffleGrouping("rabbitmq");
    builder.setBolt("recommendation", new RecommendationBolt(), 1).shuffleGrouping("observation");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxTaskParallelism(1);

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
