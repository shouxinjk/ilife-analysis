package com.ilife.analyzer.topology.money;

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
import com.ilife.analyzer.bolt.money.ClearingBolt;
import com.ilife.analyzer.bolt.money.SettleBolt;
import com.ilife.analyzer.bolt.stuff.DynamicEvaluateBolt;
import com.ilife.analyzer.spout.money.ClearedSpout;
import com.ilife.analyzer.spout.money.OrderSpout;
import com.ilife.analyzer.spout.stuff.EvaluateMeasureSpout;
import com.ilife.analyzer.spout.stuff.MeasurePropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 
 * 已清算记录按日结算汇总。
 *
 */
public class Settle extends AbstractTopology {
		String database = "forge";
		String collection = "stuff";
	    public static void main(String[] args) throws Exception {
	        new Settle().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//1，获取待结算清分记录
	    		ClearedSpout leaves = new ClearedSpout(businessConnectionProvider);
	    		
	    		//2，清分计算
	    		SettleBolt settleBolt = new SettleBolt(props,database,collection,businessConnectionProvider,analyzeConnectionProvider);

            String nodeSpout = "cleared_record_spout";
            String nodeCalcBolt = "settling_bolt";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(nodeSpout, leaves, 1);
	        builder.setBolt(nodeCalcBolt, settleBolt, 1).globalGrouping(nodeSpout);//由于采用先分组汇总，后更新状态，需要用事务方式进行处理
	        return builder.createTopology();
	    }
}
