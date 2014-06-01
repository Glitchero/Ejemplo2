package taller.storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class Bolt2 extends BaseRichBolt {

	//Declaramos variables
	

	Map<String, Integer> counters= new HashMap<String, Integer>();

	//Al final del spout cuando se cierre el cluster mostraremos los conteos.
	  
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter --");
		for(Map.Entry<String, Integer> entry : counters.entrySet()){
			System.out.println(entry.getKey()+": "+entry.getValue());
		}
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}


	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		
	//	 Si la palabra no existe en el mapa entonces la crearemos,
	//	 si existe sólo le sumamos 1
		
		 
		if(!counters.containsKey(str)){
			counters.put(str, 1);
		}else{
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}
}