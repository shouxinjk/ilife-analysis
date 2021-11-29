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
import com.ilife.analyzer.spout.stuff.PreNormDictSpout;
import com.ilife.analyzer.spout.stuff.PreNormMultiValueSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 处理多值属性marked_value。为归一化做准备。
 * 当前固定支持min、max、avg、sum四种类型，默认为sum。
 * 
 * 
 * 处理逻辑：
 * 1，从ilife_analysis.value获取多值列表。返回categoryId、propertyId、value。按照更新时间升序排列
 * 2，从mod_measure根据propertyId查询得到默认值及多值策略
 * 3.1，设置默认值，包括value、property。【注意：ope_performance由于已经分解为单值，此处不做不做设置】
 * 3.2，查询ope_performance，根据categoryId、propertyId、value得到sum、min、max、avg值
 * 		4.1，设置value 的 marked_value
 * 		4.2，设置property 的 marked_value
 * 
 * 拓扑：
 * spout---2---3.1 
 *         +---3.2
 *         		+---4.1
 *         		+---4.2
 */
public class PreNormMultiValue extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new PreNormMultiValue().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    	//1，获取待补充marked_value的数值记录
	    	PreNormMultiValueSpout propertySpout = new PreNormMultiValueSpout(analyzeConnectionProvider);
	    	
	    	//2，查询mod_measure设置的默认值及多值策略
            String sql = "select ? as value,? as value2,? as categoryId, id as propertyId, default_score as score,"
            		+ "ifnull(multi_value_func,'sum') as func,"
            		+ "if(multi_value_func='sum',1,0) as isSum, "
            		+ "if(multi_value_func='min',1,0) as isMin, "
            		+ "if(multi_value_func='max',1,0) as isMax, "
            		+ "if(multi_value_func='avg',1,0) as isAvg "
            		+ "from mod_measure where id=?";
            List<Column> queryParamColumns = Lists.newArrayList(
            		new Column("value", Types.VARCHAR),
            		new Column("value2", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR)
            		);
            String[] output_fields = {"value","value2","categoryId","propertyId","score","func","isSum","isMin","isMax","isAvg"};
            Fields outputFields = new Fields(output_fields);
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindScoreBolt = new JdbcLookupBolt(businessConnectionProvider, sql, jdbcLookupMapper);

            //3.1，如果marked_value 为空，则默认直接设置ope_performace的标注值为mod_measure上定义的默认值。避免字典中标注缺失导致阻塞
            List<Column> scoreSchemaColumns = Lists.newArrayList(
            		new Column("score", Types.DOUBLE),
            		new Column("propertyId", Types.VARCHAR));
            JdbcMapper defaultScoreUpdateMapper = new SimpleJdbcMapper(scoreSchemaColumns);
            JdbcInsertBolt jdbcDefaultScoreUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, defaultScoreUpdateMapper)
                    .withInsertQuery("update ope_performance set isReady=1,marked_value=?,update_date=now() where measure_id=? and marked_value is null");
            JdbcInsertBolt jdbcDefaultScoreUpdateBoltValue = new JdbcInsertBolt(analyzeConnectionProvider, defaultScoreUpdateMapper)
                    .withInsertQuery("update `value` set marked_value=?,modifiedOn=now() where propertyId=? and marked_value is null");
            JdbcInsertBolt jdbcDefaultScoreUpdateBoltProp = new JdbcInsertBolt(analyzeConnectionProvider, defaultScoreUpdateMapper)
                    .withInsertQuery("update property set marked_value=?,modifiedOn=now() where propertyId=? and marked_value is null");
            
	    	//3.2，从ope_performance查询得到各个分离值的sum、max、min、avg
            String functSql = "select category_id as categoryId,measure_id as propertyId, "
            		+ "? as func,? as isSum,? as isMin,? as isMax,? as isAvg,"
            		+ "? as value,"
            		+ "sum(marked_value) as sum, "
            		+ "avg(marked_value) as avg, "
            		+ "max(marked_value) as max, "
            		+ "min(marked_value) as min "
            		+ "from ope_performance "
            		+ "where category_id=? and measure_id=? and FIND_IN_SET(original_value,?)";
            List<Column> funcParamColumns = Lists.newArrayList(
            		new Column("func", Types.VARCHAR),
            		new Column("isSum", Types.BIGINT),
            		new Column("isMin", Types.BIGINT),
            		new Column("isMax", Types.BIGINT),
            		new Column("isAvg", Types.BIGINT),
            		new Column("value", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
            		new Column("value2", Types.VARCHAR));
            Fields funcOutputFields = new Fields("categoryId","propertyId","func","isSum","isMin","isMax","isAvg","value","sum","avg","max","min");
            JdbcLookupMapper funcLookupMapper = new SimpleJdbcLookupMapper(funcOutputFields, funcParamColumns);
            JdbcLookupBolt funcFindScoreBolt = new JdbcLookupBolt(businessConnectionProvider, functSql, funcLookupMapper);

            //4.1，将多值marked_value更新到分析库value
            List<Column> performanceColumns = Lists.newArrayList(
            		new Column("isSum", Types.BIGINT),
            		new Column("sum", Types.DOUBLE),
            		new Column("isAvg", Types.BIGINT),
            		new Column("avg", Types.DOUBLE),
            		new Column("isMax", Types.BIGINT),
            		new Column("max", Types.DOUBLE),
            		new Column("isMin", Types.BIGINT),
            		new Column("min", Types.DOUBLE),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
            		new Column("value", Types.VARCHAR)
            		);
            JdbcMapper valueScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcValueScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, valueScoreUpdateMapper)
                    .withInsertQuery("update `value` set modifiedOn=now(),marked_value= "
                    		+ "CASE "
                    		+ "when ?=1 then ? "
                    		+ "when ?=1 then ? "
                    		+ "when ?=1 then ? "
                    		+ "when ?=1 then ? "
                    		+ "else marked_value "
                    		+ "END "
                    		+ "where categoryId=? and propertyId=? and `value`=?");

            //4.2，将多值marked_value更新到分析库property
            JdbcMapper propertyScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcPropertyScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, propertyScoreUpdateMapper)
                    .withInsertQuery("update `property` set modifiedOn=now(),marked_value= "
                    		+ "CASE "
                    		+ "when ?=1 then ? "
                    		+ "when ?=1 then ? "
                    		+ "when ?=1 then ? "
                    		+ "when ?=1 then ? "
                    		+ "else marked_value "
                    		+ "END "
                    		+ "where categoryId=? and propertyId=? and `value`=?");
            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("prenorm-multi-value-spout", propertySpout, 1);
	        //查询得到多值策略
	        builder.setBolt("prenorm-multi-value-get-func", jdbcFindScoreBolt, 1).shuffleGrouping("prenorm-multi-value-spout");
	        //设置默认值
	        builder.setBolt("prenorm-multi-value-set-default", jdbcDefaultScoreUpdateBolt, 1).shuffleGrouping("prenorm-multi-value-get-func");
	        builder.setBolt("prenorm-multi-value-set-default-value", jdbcDefaultScoreUpdateBoltValue, 1).shuffleGrouping("prenorm-multi-value-get-func");
	        builder.setBolt("prenorm-multi-value-set-default-prop", jdbcDefaultScoreUpdateBoltProp, 1).shuffleGrouping("prenorm-multi-value-get-func");
	
	        //查询得到多值marked_value候选
	        builder.setBolt("prenorm-multi-value-find-score", funcFindScoreBolt, 1).shuffleGrouping("prenorm-multi-value-get-func");
	        builder.setBolt("prenorm-multi-value-set-score-value", jdbcValueScoreUpdateBolt, 1).shuffleGrouping("prenorm-multi-value-find-score");
	        builder.setBolt("prenorm-multi-value-set-score-prop", jdbcPropertyScoreUpdateBolt, 1).shuffleGrouping("prenorm-multi-value-find-score");
	        return builder.createTopology();
	    }
}
