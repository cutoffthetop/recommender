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
    public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
      super.open(stormConf, context, collector);
    }
  }

  public static class ObservationBolt extends ShellBolt implements IRichBolt {

    public ObservationBolt() {
      super("python", "observation.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return new HashMap<String, Object>();
    }
  }

  public static class ContentBolt extends ShellBolt implements IRichBolt {

    public ContentBolt() {
      super("python", "content.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
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
    //builder.setBolt("content", new ContentBolt(), 1).shuffleGrouping("rabbitmq");

    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxTaskParallelism(2);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("zeit-recommend", conf, builder.createTopology());

    Thread.sleep(120 * 60 * 1000);

    cluster.shutdown();
  }
}
