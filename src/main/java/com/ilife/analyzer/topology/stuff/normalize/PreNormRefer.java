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
import com.ilife.analyzer.spout.stuff.PreNormReferSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 对于通过引用标注的属性，查询得到引用对象的属性值，并计算得到标注值
 * 
 * 处理逻辑：
 * 1，读取待处理引用标注属性值
 * 2.1，先使用默认值填充,包括ope_performance, value, property
 * 2.2，根据引用categoryId及原始值，从arangodb查找对应对象，获取其客观汇总得分
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
public class PreNormRefer extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new PreNormRefer().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    	//1，获取待补充marked_value的数值记录
	    	PreNormReferSpout propertySpout = new PreNormReferSpout(businessConnectionProvider);

            //2.1，如果marked_value 为空，则默认直接设置ope_performace的标注值为mod_measure上定义的默认值。避免对象尚未入库导致阻塞
	    	//根据propertyId批量更新
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
            
	    	//2.2，从arangodb查询my_stuff得到客观评价汇总值
    		//当前通过 title 模糊匹配，仅匹配一条。直接取客观评价的score值
            //TODO 当前是逐条记录处理，需要
            String aql = "FOR doc in my_stuff filter like(doc.title,concat('%',@value,'%')) limit 1 return  {score:doc.measure.score,id:@id,categoryId:@categoryId,propertyId:@propertyId,value:@value}";
            SimpleQueryFilterCreator queryCreator = new SimpleQueryFilterCreator().withField("value","id","categoryId","propertyId");
            ArangoLookupMapper mapper = new SimpleArangoLookupMapper("score","id","categoryId","propertyId","value");
            ArangoLookupBolt arangoLookupBolt = new ArangoLookupBolt(props,"sea",aql,queryCreator,mapper);

            //3.1，将score更新到ope_performance记录
            JdbcMapper dictScoreUpdateMapper = new SimpleJdbcMapper(scoreSchemaColumns);
            JdbcInsertBolt jdbcDictScoreUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, dictScoreUpdateMapper)
                    .withInsertQuery("update ope_performance set isReady=1,marked_value=?,update_date=now() where id=?");
            
            //3.2，将score更新到分析库value
            List<Column> performanceColumns = Lists.newArrayList(
            		new Column("score", Types.VARCHAR),
//            		new Column("id", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
//            		new Column("dict", Types.VARCHAR),
            		new Column("value", Types.VARCHAR));
            JdbcMapper valueScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcValueScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, valueScoreUpdateMapper)
                    .withInsertQuery("update `value` set marked_value=?,modifiedOn=now() where categoryId=? and propertyId=? and `value`=?");

            //3.3，将score更新到分析库property
            JdbcMapper propertyScoreUpdateMapper = new SimpleJdbcMapper(performanceColumns);
            JdbcInsertBolt jdbcPropertyScoreUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, propertyScoreUpdateMapper)
                    .withInsertQuery("update `property` set marked_value=?,modifiedOn=now() where categoryId=? and propertyId=? and `value`=?");
            
            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("norm-prepare-refer-spout", propertySpout, 1);
	        //设置默认值
	        builder.setBolt("norm-prepare-refer-set-default", jdbcDefaultScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-refer-spout");
	        builder.setBolt("norm-prepare-refer-set-default-value", jdbcDefaultScoreUpdateBoltValue, 1).shuffleGrouping("norm-prepare-refer-spout");
	        builder.setBolt("norm-prepare-refer-set-default-prop", jdbcDefaultScoreUpdateBoltProp, 1).shuffleGrouping("norm-prepare-refer-spout");
	        //设置标注值
	        builder.setBolt("norm-prepare-refer-get-score", arangoLookupBolt, 1).shuffleGrouping("norm-prepare-refer-spout");
	        builder.setBolt("norm-prepare-refer-set-score", jdbcDictScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-refer-get-score");
	        builder.setBolt("norm-prepare-refer-set-score-value", jdbcValueScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-refer-get-score");
	        builder.setBolt("norm-prepare-refer-set-score-prop", jdbcPropertyScoreUpdateBolt, 1).shuffleGrouping("norm-prepare-refer-get-score");
	        return builder.createTopology();
	    }
}
