package zeit.recommend;

import backtype.storm.spout.ShellSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * zeit.recommend.PythonSpout
 *
 * This class has no description.
 *
 * Copyright: (c) 2013 by Nicolas Drebenstedt.
 * License: BSD, see LICENSE for more details.
 */

public class PythonSpout extends ShellSpout implements IRichSpout {

  private Fields fields;

  public PythonSpout(String fileName, String... fields) {
    super("python", fileName);
    this.fields = new Fields(fields);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(this.fields);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return new HashMap<String, Object>();
  }

  @Override
  public void open(Map cfg, TopologyContext ctx, SpoutOutputCollector soc) {
    super.open(cfg, ctx, soc);
  }
}