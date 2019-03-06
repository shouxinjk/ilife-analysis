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
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.ilife.analyzer.bolt.DynamicEvaluateBolt;
import com.ilife.analyzer.spout.MeasurePropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 
 * 主观维度评价：叶子节点。
 * 1，查询得到待评价叶子节点：itemKey，evaluate，type，category
 * 2，根据不同类型进行计算
 * 3，更新到对应的主观维度
 *
 */
public class EvaluateByMeasure extends AbstractTopology {
		String database = "forge";
		String collection = "stuff";
	    public static void main(String[] args) throws Exception {
	        new EvaluateByMeasure().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//1，读取叶子节点，返回：id，dimension
	    		MeasurePropertySpout leaves = new MeasurePropertySpout(analyzeConnectionProvider);
	    		
	    		//2，DynamicEvaluateBolt：分别评价各个主观维度的叶子节点，包括score及text计算
	    		DynamicEvaluateBolt dynamicEvaluateBolt = new DynamicEvaluateBolt(props,database,collection,businessConnectionProvider,analyzeConnectionProvider);

            String nodeSpout = "evaluate_measure_spout";
            String nodeCalcBolt = "evaluate_measure_clac_bolt";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(nodeSpout, leaves, 1);
	        builder.setBolt(nodeCalcBolt, dynamicEvaluateBolt, 5).shuffleGrouping(nodeSpout);
	        return builder.createTopology();
	    }
}
