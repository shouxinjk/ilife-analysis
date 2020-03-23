package com.ilife.analyzer.topology.context;

import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.arangodb.bolt.ArangoLookupBolt;
import org.apache.storm.arangodb.bolt.ArangoUpdateBolt;
import org.apache.storm.arangodb.common.QueryFilterCreator;
import org.apache.storm.arangodb.common.SimpleQueryFilterCreator;
import org.apache.storm.arangodb.common.mapper.ArangoLookupMapper;
import org.apache.storm.arangodb.common.mapper.ArangoUpdateMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoLookupMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoUpdateMapper;
import org.apache.storm.arangodb.spout.ArangoSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;

import com.ilife.analyzer.bolt.SampleBolt;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 仅计算制定Demand或Occasion是否生效。包括两部分：
 * 
 * Person：根据person profile及时间进行，将不生效的状态进行更新
 * Stuff：根据stuff profile及时间进行，将不生效的状态进行更新
 * 
 * 待定：由于当前单个商品或persona未直接关联到demands或occasion，不能做到基于单个实例评价
 * 
 * @author alexchew
 *
 */
public class EvaluateContext extends AbstractTopology {
	   private static final String SPOUT_KAFKA_CHANGE_PERSONA = "SPOUT_KAFKA_CHANGE_PERSONA".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_NEED = "BOLT_CHANGE_PERSONA_NEED".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_OCCASION = "BOLT_CHANGE_PERSONA_OCCASION".toLowerCase();
	    private static final String BOLT_CHANGE_PERSONA_DEF = "BOLT_CHANGE_PERSONA_DEF".toLowerCase();
	    
	    String database = "sea";
	    
	    public static void main(String[] args) throws Exception {
	        new EvaluateContext().execute(args);
	    }

	    @Override
	    public FlinkTopology getTopology() {
	    		//1，ArangoSpout：从Arangodb读取数据
	    		String query = "FOR doc in my_stuff_test filter doc.test=='咪咪超级大啊' limit 50 return doc";
	    		String[] fields = {"_key","source","title","_doc"};
	    		ArangoSpout arangoSpout = new ArangoSpout(props,database)
	    				.withQuery(query).withFields(fields);
	    		
	    		//2，LookupBolt：根据id查询文档
//	    		String lookupQuery = "FOR doc in my_stuff_test filter doc._key==@_key return doc";
//	    		String[] lookupFields = {"_key","title","source"};
//	    		QueryFilterCreator lookupFilter = new SimpleQueryFilterCreator();
//	    		ArangoLookupMapper lookupMapper = new SimpleArangoLookupMapper(lookupFields);
//	    		ArangoLookupBolt lookupBolt = new ArangoLookupBolt(prop,database,lookupQuery,lookupFilter,lookupMapper);
//	    		
	    		//3,检查数据
	    		String[] logFields = {"_key","test","source"};
	    		SampleBolt logBolt = new SampleBolt(props,"sea",logFields);
	    	
	    		//3，UpdateBolt：根据id更新文档
	    		String[] updateFields = {"_key","test","source"};
	    		ArangoUpdateMapper updateMapper = new SimpleArangoUpdateMapper(updateFields);
	    		ArangoUpdateBolt updateBolt = new ArangoUpdateBolt(props,database,"my_stuff_test",updateMapper);
	    		//4，KafkaBolt：发送到kafka
	    		 KafkaBolt kafkaBolt = new KafkaBolt()
	    	                .withProducerProperties(props)
	    	                .withTopicSelector(new DefaultTopicSelector("test"))
	    	                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("_key","title"));
	    		
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("SampleSpout", arangoSpout, 1);
	        builder.setBolt("SampleBolt", logBolt, 5).shuffleGrouping("SampleSpout");
	        //builder.setBolt("UpdateBolt", updateBolt, 5).shuffleGrouping("SampleBolt");
	        //builder.setBolt("KafkaBolat", kafkaBolt, 1).shuffleGrouping("SampleSpout");
	        //return builder.createTopology();
	        return FlinkTopology.createTopology(builder);
	    }
}
