package com.ilife.analyzer.topology.stuff;

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
import com.ilife.analyzer.bolt.stuff.DynamicEvaluateBolt;
import com.ilife.analyzer.bolt.stuff.FulfillmentEvaluateBolt;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 计算商品需求满足度。
 * @author alexchew
 *
 */
public class CalcFulfillment extends AbstractTopology {
	
	    String database = "sea";
	    
	    public static void main(String[] args) throws Exception {
	        new CalcFulfillment().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
    		//1，ArangoSpout：从Arangodb读取数据
    		String query = "FOR doc in my_stuff filter doc.status.satisify=='pending' update doc with {status:{satisify:'ready'}} in my_stuff limit 10 return {itemKey:doc._key,categoryId:doc.meta.category}";
    		String[] fields = {"itemKey","categoryId"};
    		ArangoSpout arangoSpout = new ArangoSpout(props,database)
    				.withQuery(query).withFields(fields);
    	
    		//2，EvaluateBolt：直接以整个文档更新
    		FulfillmentEvaluateBolt bolt = new FulfillmentEvaluateBolt(props,database,businessConnectionProvider,analyzeConnectionProvider);

	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("fulfillment-spout", arangoSpout, 1);
	        builder.setBolt("fulfillment-bolt", bolt, 1).shuffleGrouping("fulfillment-spout");
	        return builder.createTopology();
	    }
}
