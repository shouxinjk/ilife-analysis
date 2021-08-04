package com.ilife.analyzer.topology.stuff;

import java.sql.Types;
import java.util.List;

import org.apache.storm.arangodb.bolt.ArangoUpdateBolt;
import org.apache.storm.arangodb.common.mapper.SimpleArangoUpdateMapper;
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
import com.ilife.analyzer.bolt.stuff.CalcValsBolt;
import com.ilife.analyzer.spout.stuff.PropertyValsSpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 属性值变化后根据itemKey汇总更新VALS。直接查询key-value数据库，根据category、property、value查询待更新vals记录
 * 注意：当前设计中，如果属性定义发生变化则自动采用已有score重新计算 abcde xyz。计算只需要根据itemKey查询得到sum值
 * 1，读取status_vals = pending状态的property记录
 * 2，根据itemKey查询汇总的VALS值
 * 3，按照VALS数据更新item，并设置index状态为pending
 *
 *拓扑图：
 *读取待处理property记录-----根据itemKey汇总所有vals值-----更新到arangodb的item记录
 *读取待处理Property记录时，如果记录数为0， 则更新状态重新计算。注意：此处有算力浪费
 */
public class CalculateVals extends AbstractTopology {
	
		String database = "sea";
		String collection = "my_stuff";

	    public static void main(String[] args) throws Exception {
	        new CalculateVals().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
    		//1，获取status_vals状态为pending的property记录
    		PropertyValsSpout propertySpout = new PropertyValsSpout(analyzeConnectionProvider);
    		
    		//2，根据itemKey获取所有vals值汇总
            String sql = "select ? as _key,? as category,? as propertyId,? as value,sum(score*weight*alpha) as a,sum(score*weight*beta) as b,sum(score*weight*gamma) as c,sum(score*weight*delte) as d,sum(score*weight*epsilon) as e,sum(score*weight*zeta) as x,sum(score*weight*eta) as y,sum(score*weight*theta) as z from property where itemKey=?";
            List<Column> queryParamColumns = Lists.newArrayList(
            		new Column("_key", Types.VARCHAR),
            		new Column("category", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
            		new Column("value", Types.VARCHAR),
            		new Column("itemKey2", Types.VARCHAR));
            String[] fields = {"_key","category","propertyId","value","a","b","c","d","e","x","y","z"};
            Fields fieldArray = new Fields(fields);
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(fieldArray, queryParamColumns);
            JdbcLookupBolt jdbcFindScoreBolt = new JdbcLookupBolt(analyzeConnectionProvider, sql, jdbcLookupMapper);

            //3，更新item doc
            SimpleArangoUpdateMapper docMapper = new SimpleArangoUpdateMapper("_key","a","b","c","d","e","x","y","z");
            ArangoUpdateBolt updateValsBolt = new ArangoUpdateBolt(props,database,collection,docMapper);
//            CalcValsBolt updateValsBolt = new CalcValsBolt(props,database,collection,businessConnectionProvider,analyzeConnectionProvider);
            
            //4，将score、rank更新到property记录
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("_key", Types.VARCHAR),
            		new Column("category", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
            		new Column("value", Types.VARCHAR)//,
//            		new Column("a", Types.DOUBLE),
//            		new Column("b", Types.DOUBLE),
//            		new Column("c", Types.DOUBLE),
//            		new Column("d", Types.DOUBLE),
//            		new Column("e", Types.DOUBLE),
//            		new Column("x", Types.DOUBLE),
//            		new Column("y", Types.DOUBLE),
//            		new Column("z", Types.DOUBLE)
            		);
            JdbcMapper updateMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateMapper)
                    .withInsertQuery("update property set modifiedOn=now(),status_vals='ready' where itemKey=? and category=? and propertyId=? and value=?");

            //装配topology
            String SPOUT = "vals_spout";
            String SUMBOLT = "vals_sum_bolt";
            String UPDATEDOCBOLT = "vals_update_bolt";
            String UPDATEPROPERTYBOLT = "update_property_status";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(SPOUT, propertySpout, 1);
	        builder.setBolt(SUMBOLT, jdbcFindScoreBolt, 1).shuffleGrouping(SPOUT);
	        builder.setBolt(UPDATEDOCBOLT, updateValsBolt, 1).shuffleGrouping(SUMBOLT);
	        builder.setBolt(UPDATEPROPERTYBOLT, jdbcUpdateBolt, 1).shuffleGrouping(SUMBOLT);
	        return builder.createTopology();
	    }
}
