package com.ilife.analyzer.topology.person;

import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.arangodb.spout.ArangoSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import com.ilife.analyzer.bolt.person.ScorePersonaBolt;
import com.ilife.analyzer.spout.person.PersonaSpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 评价用户对已定义persona的匹配得分
 * 
 * Topology:
 * 
 * PersonaSpout----ScorePersonaBolt
 * 
 * @author alexchew
 *
 */
public class EvaluatePersona  extends AbstractTopology {
	   private static final String SPOUT_KAFKA_CHANGE_PERSONA = "SPOUT_KAFKA_CHANGE_PERSONA".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_NEED = "BOLT_CHANGE_PERSONA_NEED".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_OCCASION = "BOLT_CHANGE_PERSONA_OCCASION".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_DEF = "BOLT_CHANGE_PERSONA_DEF".toLowerCase();
	    
	    String database = "forge";
	    
	    public static void main(String[] args) throws Exception {
	        new EvaluatePersona().execute(args);
	    }

	    @Override
	    public FlinkTopology getTopology() {
	    		//1，Spout:获取待处理user_persona记录
	    		PersonaSpout spout= new PersonaSpout(analyzeConnectionProvider);
	    		//2，Bolt:分别计算阶层、阶段、分群得分
	    		ScorePersonaBolt bolt = new ScorePersonaBolt(props,"sea","forge",businessConnectionProvider,analyzeConnectionProvider);
	    		
	    		String idSpout = "evaluate-persona_spout";
	    		String idBolt="evaluate-persona_bolt";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(idSpout, spout, 1);
	        builder.setBolt(idBolt, bolt, 1).shuffleGrouping(idSpout);
	        return FlinkTopology.createTopology(builder);
	    }
	}
