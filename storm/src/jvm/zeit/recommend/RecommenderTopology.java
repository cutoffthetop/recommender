package zeit.recommend;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.ShellSpout;
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
      declarer.declare(new Fields("word"));
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

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("rabbit", new RabbitMQ(), 1);
    builder.setBolt("split", new WordCount(), 1).shuffleGrouping("rabbit");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", conf, builder.createTopology());

    Thread.sleep(10000);

    cluster.shutdown();
  }
}
