package taller.storm;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout {
  
  private SpoutOutputCollector collector;
 
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	 this.collector = collector;
  }

  @Override
  public void nextTuple() {
	Utils.sleep(100);  
	final Random rand = new Random();
    String[] sentences = new String[]{ "la vaca brincó sobre la luna", "una manzana al día te mantiene alejado del doctor",
    "big data es el futuro", "blanca nieve y los 7 enanos", "estoy programando en R" };
    String sentence = sentences[rand.nextInt(sentences.length)];
    collector.emit(new Values(sentence));
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("oracion"));
  }

}