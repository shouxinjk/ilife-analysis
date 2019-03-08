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
import com.ilife.analyzer.spout.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 归一化处理。直接查询key-value数据库，根据category、property、value查询标注库是否已经有标注记录，如果有则使用标注值填写归一化值
 * 1，读取pending状态的property记录
 * 2，读取对应的value记录
 * 3，将对应的归一化rank、value写入property记录，并更新状态
 *
 *拓扑图：
 *读取待处理property记录-----读取对应value记录-----更新对应property记录
 *读取待处理Property记录时，如果记录数为0， 则更新状态
 */
public class Normalize extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new Normalize().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//1，获取pending状态property记录，内部将处理
	    		PropertySpout propertySpout = new PropertySpout(analyzeConnectionProvider);
	    		
	    		//2，读取对应的标注value记录
            String sql = "select score,rank,category,property,value from value where category=? and property=? and value=? and status='ready'";
            List<Column> queryParamColumns = Lists.newArrayList(
            		new Column("category", Types.VARCHAR),
            		new Column("property", Types.VARCHAR),
            		new Column("value", Types.VARCHAR));
            String[] output_fields = {"score","rank","category","property","value"};
            Fields outputFields = new Fields(output_fields);
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindScoreBolt = new JdbcLookupBolt(analyzeConnectionProvider, sql, jdbcLookupMapper);

            //3，将score、rank更新到property记录
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("score", Types.DOUBLE),
            		new Column("rank", Types.INTEGER),
            		new Column("category", Types.VARCHAR),
            		new Column("property", Types.VARCHAR),
            		new Column("value", Types.VARCHAR));
            JdbcMapper updateMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateMapper)
                    .withInsertQuery("update property set score=?,rank=?,modifiedOn=now(),status='ready' where category=? and property=? and value=?");

            //装配topology
            String spout = "normalize_spout";
            String findScoreBolt = "normalize_find_score";
            String updateProperyBolt = "normalize_update_property_score";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(spout, propertySpout, 1);
	        builder.setBolt(findScoreBolt, jdbcFindScoreBolt, 5).shuffleGrouping(spout);
	        builder.setBolt(updateProperyBolt, jdbcUpdateBolt, 1).shuffleGrouping(findScoreBolt);
	        return builder.createTopology();
	    }
}
