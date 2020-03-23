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
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.ilife.analyzer.spout.person.MeasureDimensionSpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 
 * 客观维度评价：非叶子节点。
 * 1，查询得到待评价节点：userKey，dimension
 * 2，查询该子节点所有下级节点的dimension及weight，并进行加权汇总 sum(weight*score)
 * 3，将得分更新到measure：update measure set score=?,modifiedOn=now(),status="ready" where userKey=? and dimension=?
 *
 */
public class MeasureByDimension extends AbstractTopology {
	    
	    public static void main(String[] args) throws Exception {
	        new MeasureByDimension().execute(args);
	    }

	    @Override
	    public FlinkTopology getTopology() {
	    		//1，读取非叶子节点，返回：userKey，dimension
	    		MeasureDimensionSpout leaves = new MeasureDimensionSpout(analyzeConnectionProvider);
	    		
	    		//2，JdbcLookupBolt：查询并汇总叶子节点关联属性的加权得分，对应于特定userKey，并从属于当前dimension
	    		//注意：当前默认采用加权汇总算法，如果采用单独算法，需要定义相应的Bolt
            String sql = "select sum(weight*score) as score, ? as userKey, ? as dimension from user_measure where userKey=? and parent=?";
            List<Column> queryParamColumns = Lists.newArrayList(
            		new Column("userKey", Types.VARCHAR),
            		new Column("dimension", Types.VARCHAR),
            		new Column("userKey2", Types.VARCHAR),
            		new Column("dimension2", Types.VARCHAR));
            String[] output_fields = {"score","userKey","dimension"};
            Fields outputFields = new Fields(output_fields);//输出维度，并带有原始JSON
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindDimensionsBolt = new JdbcLookupBolt(analyzeConnectionProvider, sql, jdbcLookupMapper);

            //3，更新measure叶子节点得分
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("score", Types.DOUBLE),
            		new Column("userKey", Types.VARCHAR),
            		new Column("dimension", Types.VARCHAR));
            JdbcMapper updateMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdateMeasureBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateMapper)
                    .withInsertQuery("update user_measure set score=?,modifiedOn=now(),status='ready' where userKey=? and dimension=?");

            String nodeSpout = "measure_user_dimension_spout";
            String nodeCalcScore = "measure_user_dimension_calc_score";
            String nodeUpdateScore = "measure_user_dimension_update_score";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(nodeSpout, leaves, 1);
	        builder.setBolt(nodeCalcScore, jdbcFindDimensionsBolt, 5).shuffleGrouping(nodeSpout);
	        builder.setBolt(nodeUpdateScore, jdbcUpdateMeasureBolt, 1).shuffleGrouping(nodeCalcScore);
	        return FlinkTopology.createTopology(builder);
	    }
}
