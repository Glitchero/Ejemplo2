package taller.storm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Bolt1 extends BaseRichBolt {

	private OutputCollector collector;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	
	public void execute(Tuple input) {
     // String sentence = input.getString(0);  
        String sentence = input.getStringByField("oracion");
        String[] words = sentence.split(" "); //Obtenemos las palabras de la oración y las metemos a un vector.
        for(String word : words){
            word = word.trim();   //Quitamos los espacios iniciales y finales, ejem: "  hola  "" => "hola"
            if(!word.isEmpty()){
                word = word.toLowerCase();   //Convertimos todo a minúsculas
                collector.emit(new Values(word));
            }
        }
	}
		
	 // Este bolt sólo emitirá una palabra
	 
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}