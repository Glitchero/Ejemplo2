package taller.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Fields;

public class Topologia {

	
	public static void main(String[] args) throws Exception {
		
		//Definición de la topología
		TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("oracionesaleatorias", new Spout(), 1);        
        builder.setBolt("normalizador", new Bolt1(), 1)
                .shuffleGrouping("oracionesaleatorias");
        builder.setBolt("contador", new Bolt2(), 2)
                .fieldsGrouping("normalizador", new Fields("word"));
        //Con fieldsGrouping contralamos como se envían las tuplas a los bolt
        //En este ejemplo siempre enviamos las tuplas con una palabra dada a la misma instancia creada por el Bolt2.
        
        //Configuración
        Config conf = new Config();
        conf.setDebug(false);
        
        //Creamos el cluster local
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("prueba", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("prueba");
        cluster.shutdown();  
        
	}

}