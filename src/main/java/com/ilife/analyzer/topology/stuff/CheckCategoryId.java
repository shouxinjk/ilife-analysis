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
import com.ilife.analyzer.spout.stuff.CategoryIdSpout;
import com.ilife.analyzer.spout.stuff.PropertyIdSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 根据category名称得到对应的categoryId。用于准备进行维度分析
 * 1，从分析库property内读取categoryId为null的记录，返回category
 * 2，根据category名称从业务库内读取categoryId
 * 3，将categoryId更新到分析库property表内
 *
 */
public class CheckCategoryId extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new CheckCategoryId().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
    		//1，获取id为空的property记录，将匹配填写propertyId
    		CategoryIdSpout propertySpout = new CategoryIdSpout(analyzeConnectionProvider);
    		
    		//2，直接从platform_categories读取映射记录，根据platform及category名称匹配。目录名称匹配规则：末级节点匹配，或全路径匹配
            String aql = "FOR doc in platform_categories filter doc.source==@platform and (doc.name==@category or concat(doc.names)==@category) limit 1 return  {mappingId:doc.mappingId,mappingName:doc.mappingName,category:doc.name,platform:doc.source}";
            SimpleQueryFilterCreator queryCreator = new SimpleQueryFilterCreator().withField("platform","category");
            String[] mapping_fields = {"mappingId","mappingName","category","platform"};
            ArangoLookupMapper mapper = new SimpleArangoLookupMapper(mapping_fields);
            ArangoLookupBolt arangoLookupBolt = new ArangoLookupBolt(props,"sea",aql,queryCreator,mapper);
            
            //3，将id更新到property记录
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("mappingId", Types.VARCHAR),
            		new Column("mappingName", Types.VARCHAR),
            		new Column("category", Types.VARCHAR),
            		new Column("platform", Types.VARCHAR));
            JdbcMapper updateMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdateBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateMapper)
                    .withInsertQuery("update property set categoryId=?,mappingName=? where category=? and platform=?");
            
            //4，根据匹配的categoryIdproperty装载到platform_properties，便于建立属性映射。仅选取props.xxx 属性
            //cid：原始平台目录ID；name：原始属性名称，即props.xxx的xxx部分；source：来源平台
            String sqlFindProps = "select platform as `source`,substring(property,7) as name,md5(concat(platform,substring(property,7))) as _key from property where platform=? and category=? and substring(property,1,6)='props.'";
            List<Column> queryParamColumns = Lists.newArrayList(
            		//new Column("mappingId", Types.VARCHAR),//映射的标准目录ID:platform_properties内使用原始平台目录ID，不需要映射目录Id
            		new Column("platform", Types.VARCHAR),
            		new Column("category", Types.VARCHAR));
            String[] prop_fields = {"source","name","_key"};
            Fields outputFields = new Fields(prop_fields);
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindPropsBolt = new JdbcLookupBolt(analyzeConnectionProvider, sqlFindProps, jdbcLookupMapper);
            //5，写入platform_properties
    		ArangoMapper arangoMapper = new SimpleArangoMapper(prop_fields);
    		ArangoInsertBolt insertPropsBolt = new ArangoInsertBolt(props,"sea","platform_properties",arangoMapper);

            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("check_categoryId_spout", propertySpout, 1);
	        builder.setBolt("check_categoryId_find_id", arangoLookupBolt, 1).shuffleGrouping("check_categoryId_spout");
	        builder.setBolt("check_categoryId_update_id", jdbcUpdateBolt, 1).shuffleGrouping("check_categoryId_find_id");
	        builder.setBolt("query_props_by_category", jdbcFindPropsBolt, 1).shuffleGrouping("check_categoryId_find_id");
	        builder.setBolt("insert_platform_props", insertPropsBolt, 1).shuffleGrouping("query_props_by_category");
	        return builder.createTopology();
	    }
}
