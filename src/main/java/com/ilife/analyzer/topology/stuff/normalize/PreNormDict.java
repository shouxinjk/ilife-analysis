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
import com.ilife.analyzer.bolt.stuff.LabelByDictBolt;
import com.ilife.analyzer.spout.stuff.PreNormDictSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 对于通过字典标注的属性，根据原始值，补充字典标注值。为归一化做准备。
 * 
 * 处理逻辑：
 * 1，从ilife.ope_performance及ilife.mod_measure 获取待字典标注属性值列表。得到id(数值id)、categoryId、propertyId、value（原始值）、dict(字典表名称)、score(属性上的给出的标注值)。其中marked_value为null的优先处理
 * 2.1，如果标注值为空，则设置为属性上的默认值，避免字典表缺失阻塞。包括ope_performance, value ,property
 * 2.2，从字典表内，根据属性值查询得到 score
 * 		3.1，根据categoryId、propertyId、属性值更新ilife.ope_performance的marked_value
 * 		3.2，根据categoryId、propertyId、属性值更新ilife_analysis.value的marked_value
 * 		3.3，根据categoryId、propertyId、属性值更新ilife_analysis.property的marked_value
 * 
 * 拓扑：
 * spout---2.1
 *     +---2.2---3.1 
 *           +---3.2
 *           +---3.3
 */
public class PreNormDict extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new PreNormDict().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    	//1，获取待补充marked_value的数值记录
	    	PreNormDictSpout propertySpout = new PreNormDictSpout(businessConnectionProvider);

            //2.1，如果marked_value 为空，则默认直接设置ope_performace的标注值为mod_measure上定义的默认值。避免字典中标注缺失导致阻塞
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
            
	    	//2.2，从字典查询标注值的score
            LabelByDictBolt labelByDictBolt = new LabelByDictBolt(props,"",businessConnectionProvider,analyzeConnectionProvider);
            
            //3.1，将score更新到ope_performance记录
            List<Column> performanceColumns = Lists.newArrayList(
            		new Column("score", Types.DOUBLE),
//            		new Column("id", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
//            		new Column("dict", Types.VARCHAR),
            		new Column("value", Types.VARCHAR));
            JdbcMapper dictScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcDictScoreUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, dictScoreUpdateMapper)
                    .withInsertQuery("update ope_performance set marked_value=?,update_date=now() where category_id=? and measure_id=? and original_value=?");

            //3.2，将score更新到分析库value
            JdbcMapper valueScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcValueScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, valueScoreUpdateMapper)
                    .withInsertQuery("update `value` set marked_value=?,modifiedOn=now() where categoryId=? and propertyId=? and `value`=?");

            //3.3，将score更新到分析库property
            JdbcMapper propertyScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcPropertyScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, propertyScoreUpdateMapper)
                    .withInsertQuery("update `property` set marked_value=?,modifiedOn=now() where categoryId=? and propertyId=? and `value`=?");
            
            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("norm-prepare-dict-spout", propertySpout, 1);
	        //设置默认值
	        builder.setBolt("norm-prepare-dict-set-default", jdbcDefaultScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-dict-spout");
	        builder.setBolt("norm-prepare-dict-set-default-value", jdbcDefaultScoreUpdateBoltValue, 1).shuffleGrouping("norm-prepare-dict-spout");
	        builder.setBolt("norm-prepare-dict-set-default-prop", jdbcDefaultScoreUpdateBoltProp, 1).shuffleGrouping("norm-prepare-dict-spout");
	        //设置标注值
	        builder.setBolt("norm-prepare-dict-get-score", labelByDictBolt, 1).shuffleGrouping("norm-prepare-dict-spout");
	        builder.setBolt("norm-prepare-dict-set-score", jdbcDictScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-dict-get-score");
	        builder.setBolt("norm-prepare-dict-set-score-value", jdbcValueScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-dict-get-score");
	        builder.setBolt("norm-prepare-dict-set-score-prop", jdbcPropertyScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-dict-get-score");
	        return builder.createTopology();
	    }
}
