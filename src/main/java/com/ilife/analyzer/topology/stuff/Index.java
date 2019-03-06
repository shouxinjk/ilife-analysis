package com.ilife.analyzer.topology.stuff;

import java.sql.Types;
import java.util.List;

import org.apache.storm.arangodb.bolt.ArangoInsertBolt;
import org.apache.storm.arangodb.bolt.ArangoLookupBolt;
import org.apache.storm.arangodb.bolt.ArangoUpdateBolt;
import org.apache.storm.arangodb.common.QueryFilterCreator;
import org.apache.storm.arangodb.common.SimpleQueryFilterCreator;
import org.apache.storm.arangodb.common.mapper.ArangoLookupMapper;
import org.apache.storm.arangodb.common.mapper.ArangoMapper;
import org.apache.storm.arangodb.common.mapper.ArangoUpdateMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoLookupMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoUpdateMapper;
import org.apache.storm.arangodb.spout.ArangoSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.ilife.analyzer.bolt.CreateMeasureTaskBolt;
import com.ilife.analyzer.bolt.JsonParseBolt;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 提交索引： 按照索引状态进行更新。如果指定记录的索引状态为pending，则提交索引
 * 
 * 1，读取分析库中索引状态为pending的记录
 * 2，提交内容到索引库
 */
public class Index extends AbstractTopology {
	    String arango_harvest = "sea";//采集库，存放原始采集数据
	    String arango_analyze = "forge";//分析库，存放分析结果
	    
	    public static void main(String[] args) throws Exception {
	        new Index().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//1，ArangoSpout：从arangodb读取状态为pending的初始数据，读取后即更新状态为ready
	    		String query = "FOR doc in my_stuff filter doc.status == \"pending\" update doc with { status: \"ready\" } in my_stuff limit 50 return OLD";
	    		String[] fields = {"_doc"};
	    		ArangoSpout arangoSpout = new ArangoSpout(props,arango_analyze)
	    				.withQuery(query).withFields(fields);
	    		
	    		//2，KafkaBolt：内容提交到索引库 //TODO: 当前将所有内容放在_doc字段，需要进行区分
	    		KafkaBolt kafkaBolt = new KafkaBolt()
	    		        .withProducerProperties(props)
	    		        .withTopicSelector(new DefaultTopicSelector("stuff"))
	    		        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
            
            //构建Topology
            String nameSpout = "index_spout_load_doc";
            String nameKafkaBolt = "index_kafka_bolt";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(nameSpout, arangoSpout, 1);
	        builder.setBolt(nameKafkaBolt, kafkaBolt, 1).shuffleGrouping(nameSpout);
	        return builder.createTopology();
	    }
}
