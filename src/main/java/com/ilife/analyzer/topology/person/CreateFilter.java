package com.ilife.analyzer.topology.person;

import java.sql.Types;
import java.util.List;

import org.apache.flink.storm.api.FlinkTopology;
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
import com.ilife.analyzer.bolt.JsonParseBolt;
import com.ilife.analyzer.bolt.person.BuildFilterBolt;
import com.ilife.analyzer.bolt.stuff.CreateMeasureTaskBolt;
import com.ilife.analyzer.spout.person.FilterSpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 建立个性化Filter。根据用户属性组建附加Filter，用于精准推荐。
 * 1，构建bool query：jdbcbolt从analyze:user_query查询得到，并按照must、must not、filter、should构建布尔查询，输入userKey
 * 2，构建function score(step 1)：jdbcbolt从analyze:user_persona查询得到最匹配persona，如果没有则使用默认值。输出userKey，personaId
 * 3，构建function score(step 2)：jdbcbolt从business:mod_persona查询得到vals，构建8个gauss衰减函数，输出
 * 4，构建关注度等固定function：直接硬编码
 * 5，更新到user_user:arangobolt直接更新query字段
 * 
 */
public class CreateFilter extends AbstractTopology {
	    String arango_harvest = "sea";//采集库，存放原始采集数据
	    String arango_analyze = "forge";//分析库，存放分析结果
	    
	    public static void main(String[] args) throws Exception {
	        new CreateFilter().execute(args);
	    }

	    @Override
	    public FlinkTopology getTopology() {
	    		//1，FilterSpout：读取状态为pending的初始数据，读取后即更新状态为ready
	    		FilterSpout spout = new FilterSpout(analyzeConnectionProvider,"pending");
	    		
	    		//2，FilterBolt：构建用户过滤器
	    		BuildFilterBolt bolt = new BuildFilterBolt(props,"sea","user_users",businessConnectionProvider,analyzeConnectionProvider);
            
            //构建Topology
            String nameSpout = "create_filter_spout_pending_tasks";
            String nameBolt = "create_filter_bolt_calculate";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(nameSpout, spout, 1);
	        builder.setBolt(nameBolt, bolt, 5).shuffleGrouping(nameSpout);
	        return FlinkTopology.createTopology(builder);
	    }
}
