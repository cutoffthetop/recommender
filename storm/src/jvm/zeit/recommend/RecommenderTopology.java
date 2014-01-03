package zeit.recommend;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.spout.SpoutOutputCollector;

import java.util.HashMap;
import java.util.Map;

public class RecommenderTopology {

  public static class RabbitMQ extends ShellSpout implements IRichSpout {

    public RabbitMQ() {
      super("python", "rabbit.py");
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

  public static class ESIndex extends ShellBolt implements IRichBolt {

    public ESIndex() {
      super("python", "index.py");
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

    builder.setSpout("rabbitmq", new RabbitMQ(), 1);
    builder.setBolt("esindex", new ESIndex(), 1).shuffleGrouping("rabbitmq");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("zeit-recommend", conf, builder.createTopology());

    Thread.sleep(20000);

    cluster.shutdown();
  }
}
