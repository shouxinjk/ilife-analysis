package com.ilife.analyzer.topology.stuff.normalize;

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
import com.ilife.analyzer.spout.stuff.PreNormAutoSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 对于自动标注的属性，根据原始值，解析为double型数值，填写为marked_value。为归一化做准备。
 * 
 * 处理逻辑：
 * 1，读取待处理自动标注属性
 * 2.1，如果标注值为空，则设置为属性上的默认值，避免字典表缺失阻塞，包括ope_performance, value, property
 * 2.2，判断标注值是否为数值，如果则转换更新, 包括ope_performance, value, property
 * 
 * 拓扑：
 * spout---2.1
 *     +---2.2
 */
public class PreNormAuto extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new PreNormAuto().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    	//1，获取待补充marked_value的数值记录
	    	PreNormAutoSpout propertySpout = new PreNormAutoSpout(businessConnectionProvider);

            //2.1，如果marked_value 为空，则默认直接设置ope_performace的标注值为mod_measure上定义的默认值。避免字典中标注缺失导致阻塞
	    	//是批量操作，根据propertyId直接批量更新
            List<Column> scoreSchemaColumns = Lists.newArrayList(
            		new Column("score", Types.DOUBLE),
            		new Column("propertyId", Types.VARCHAR));
            JdbcMapper defaultScoreUpdateMapper = new SimpleJdbcMapper(scoreSchemaColumns);
            JdbcInsertBolt jdbcDefaultScoreUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, defaultScoreUpdateMapper)
                    .withInsertQuery("update ope_performance set marked_value=?,update_date=now() where measure_id=? and marked_value is null");
            JdbcInsertBolt jdbcDefaultScoreUpdateBoltValue = new JdbcInsertBolt(analyzeConnectionProvider, defaultScoreUpdateMapper)
                    .withInsertQuery("update `value` set marked_value=?,modifiedOn=now() where propertyId=? and marked_value is null");
            JdbcInsertBolt jdbcDefaultScoreUpdateBoltProp = new JdbcInsertBolt(analyzeConnectionProvider, defaultScoreUpdateMapper)
                    .withInsertQuery("update property set marked_value=?,modifiedOn=now() where propertyId=? and marked_value is null");

	    	//2.2，判断原始值，如果是数值，则直接更新。仅对于original_value为数值的记录进行
            //是批量操作，根据propertyId即可完成，不是逐行进行
            List<Column> performanceColumns = Lists.newArrayList(
//            		new Column("score", Types.VARCHAR),
//            		new Column("id", Types.VARCHAR),
//            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR)
//            		new Column("dict", Types.VARCHAR),
//            		new Column("value", Types.VARCHAR)
            		);
            JdbcMapper autoScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcAutoScoreUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, autoScoreUpdateMapper)
                    .withInsertQuery("update ope_performance set marked_value=convert(original_value,decimal(10,2)),update_date=now() where (original_value REGEXP '[^0-9.]')=0 and measure_id=?");
            
            //2.3，将score更新到分析库value
            JdbcMapper valueScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcValueScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, valueScoreUpdateMapper)
                    .withInsertQuery("update `value` set marked_value=convert(`value`,decimal(10,2)),modifiedOn=now() where (`value` REGEXP '[^0-9.]')=0 and propertyId=?");

            //2.4，将score更新到分析库property
            JdbcMapper propertyScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcPropertyScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, propertyScoreUpdateMapper)
                    .withInsertQuery("update `property` set marked_value=convert(`value`,decimal(10,2)),modifiedOn=now() where (`value` REGEXP '[^0-9.]')=0  and propertyId=?");
            
            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("norm-prepare-auto-spout", propertySpout, 1);
	        //设置默认
	        builder.setBolt("norm-prepare-auto-set-default", jdbcDefaultScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-auto-spout");
	        builder.setBolt("norm-prepare-auto-set-default-value", jdbcDefaultScoreUpdateBoltValue, 1).shuffleGrouping("norm-prepare-auto-spout");
	        builder.setBolt("norm-prepare-auto-set-default-prop", jdbcDefaultScoreUpdateBoltProp, 1).shuffleGrouping("norm-prepare-auto-spout");
	        //自动计算更新
	        builder.setBolt("norm-prepare-auto-set-score", jdbcAutoScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-auto-spout");
	        builder.setBolt("norm-prepare-auto-set-score-value", jdbcValueScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-auto-spout");
	        builder.setBolt("norm-prepare-auto-set-score-prop", jdbcPropertyScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-auto-spout");
	        return builder.createTopology();
	    }
}
