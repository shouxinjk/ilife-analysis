package com.ilife.analyzer.topology.person;

import org.apache.storm.arangodb.spout.ArangoSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import com.ilife.analyzer.topology.AbstractTopology;

public class CheckPersona  extends AbstractTopology {
	   private static final String SPOUT_KAFKA_CHANGE_PERSONA = "SPOUT_KAFKA_CHANGE_PERSONA".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_NEED = "BOLT_CHANGE_PERSONA_NEED".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_OCCASION = "BOLT_CHANGE_PERSONA_OCCASION".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_DEF = "BOLT_CHANGE_PERSONA_DEF".toLowerCase();
	    
	    String database = "forge";
	    
	    public static void main(String[] args) throws Exception {
	        new CheckPersona().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//1，从person获取待更新列表，筛选逻辑：
	    		//a) person的大版本小于关联persona的大版本，且【注意：实际上该比较不能生效，不能直接通过persona版本进行比较】
	    		//b) build=0 || 数据状态=changed
	    		//c) 任务状态=new 
	    		String query = "FOR doc in persons filter build=0 && task='new' update with {task:'processing'} return NEW";
	    		String[] fields = {"_key","VALS",""};
	    		ArangoSpout arangoSpout = new ArangoSpout(props,database)
	    				.withQuery(query).withFields(fields);
	    		
	    		//2，更新处理状态为processing
	    		//ok 已经在查询时直接修改
	    		
	    		//3，重新计算Person的Persona
	    		//查询得到VALS向量，与所有Persona判定规则进行匹配，选择最匹配的persona进行更新
	    		
	    		//4，更新Person的Persona属性
	    		//5，更新person meta数据。build = build+1，数据状态=processed，任务状态=done
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(SPOUT_KAFKA_CHANGE_PERSONA, arangoSpout, 1);
//	        builder.setBolt(SQL_DELETE_UNMATCH_CHECKUP_ITEM, jdbcCleanCheckupItemBolt, 1).shuffleGrouping(PENDING_CHECKUP_ITEM_SPOUT);
	        return builder.createTopology();
	    }
	}
