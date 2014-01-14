package zeit.recommend;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

public class PythonBolt extends ShellBolt implements IRichBolt {

  private Fields fields;

  public PythonBolt(String fileName, String... fields) {
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
}