package com.ilife.analyzer.topology.stuff;

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
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.ilife.analyzer.spout.stuff.PropertyIdSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 根据category、property得到对应的propertyId。用于准备进行维度分析
 * 1，从分析库property内读取propertyId为null的记录，返回category、property
 * 2，根据category、proptery从业务库内读取propertyId
 * 3，将propertyId更新到分析库property表内
 *
 */
public class CheckPropertyId extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new CheckPropertyId().execute(args);
	    }

	    @Override
	    public FlinkTopology getTopology() {
	    		//1，获取id为空的property记录，将匹配填写propertyId
	    		PropertyIdSpout propertySpout = new PropertyIdSpout(analyzeConnectionProvider);
	    		
	    		//2，读取对应的标注value记录
            String sql = "select id as propertyId,category as categoryId,property as propertyName from mod_measure where category=? and property=?";
            List<Column> queryParamColumns = Lists.newArrayList(
            		//new Column("propertyId", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyName", Types.VARCHAR));
            String[] output_fields = {"propertyId","categoryId","propertyName"};
            Fields outputFields = new Fields(output_fields);
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindScoreBolt = new JdbcLookupBolt(businessConnectionProvider, sql, jdbcLookupMapper);

            //3，将id更新到property记录
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("propertyId", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyName", Types.VARCHAR));
            JdbcMapper updateMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateMapper)
                    .withInsertQuery("update property set propertyId=? where categoryId=? and property=?");

            //装配topology
            String spout = "check_propertyId_spout";
            String findScoreBolt = "check_propertyId_find_id";
            String updateProperyBolt = "check_propertyId_update_id";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(spout, propertySpout, 1);
	        builder.setBolt(findScoreBolt, jdbcFindScoreBolt, 5).shuffleGrouping(spout);
	        builder.setBolt(updateProperyBolt, jdbcUpdateBolt, 1).shuffleGrouping(findScoreBolt);
	        return FlinkTopology.createTopology(builder);
	    }
}
