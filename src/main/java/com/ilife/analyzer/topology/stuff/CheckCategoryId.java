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
import com.ilife.analyzer.bolt.stuff.ChangeCategoryBolt;
import com.ilife.analyzer.spout.stuff.CategoryIdSpout;
import com.ilife.analyzer.spout.stuff.PropertyIdSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 
 * @deprecated
 * 
 * 根据category名称查询得到映射的categoryId。
 * 
 * 1，从分析库property内读取categoryId为null的记录，返回category、platform
 * 2，根据platform、category名称从platform_categories读取mappingId及mappingName
 * 3.1，将mappingId、mappingName更新到分析库property及value表内
 * 3.2，将mappingId更新到my_stuff的meta.category字段
 *
 */
public class CheckCategoryId extends AbstractTopology {
	String database = "sea";
	String collection = "my_stuff";
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
        
        //3.0 将标准目录ID更新到my_stuff。是批量操作
        ChangeCategoryBolt changeCategoryBolt = new ChangeCategoryBolt(props,database,collection,businessConnectionProvider,analyzeConnectionProvider);
        
        //3.1，将id更新到property记录
        List<Column> propertySchemaColumns = Lists.newArrayList(
        		new Column("mappingId", Types.VARCHAR),
        		new Column("mappingName", Types.VARCHAR),
        		new Column("category", Types.VARCHAR),
        		new Column("platform", Types.VARCHAR));
        JdbcMapper updatePropertyMapper = new SimpleJdbcMapper(propertySchemaColumns);
        JdbcInsertBolt jdbcUpdatePropertyBolt = new JdbcInsertBolt(analyzeConnectionProvider, updatePropertyMapper)
                .withInsertQuery("update property set categoryId=?,mappingName=?,modifiedOn=now() where category=? and platform=?");
        
        //3.2，将id更新到value记录
        List<Column> valueSchemaColumns = Lists.newArrayList(
        		new Column("mappingId", Types.VARCHAR),
//            		new Column("mappingName", Types.VARCHAR),//不需要mappingName
        		new Column("category", Types.VARCHAR),
        		new Column("platform", Types.VARCHAR));
        JdbcMapper updateValueMapper = new SimpleJdbcMapper(valueSchemaColumns);
        JdbcInsertBolt jdbcUpdateValueBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateValueMapper)
                .withInsertQuery("update `value` set categoryId=?,modifiedOn=now() where category=? and platform=?");
        
        //3.3，根据匹配的categoryIdproperty装载到platform_properties，便于建立属性映射。仅选取props.xxx 属性
        //cid：原始平台目录ID；name：原始属性名称，即props.xxx的xxx部分；source：来源平台
        //3.3.1，查询得到类目下的property
        String sqlFindProps = "select platform as `source`,property as name,md5(concat(platform,property)) as _key from property where platform=? and category=?";
        List<Column> queryParamColumns = Lists.newArrayList(
        		//new Column("mappingId", Types.VARCHAR),//映射的标准目录ID:platform_properties内使用原始平台目录ID，不需要映射目录Id
        		new Column("platform", Types.VARCHAR),
        		new Column("category", Types.VARCHAR));
        String[] prop_fields = {"source","name","_key"};
        Fields outputFields = new Fields(prop_fields);
        JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt jdbcFindPropsBolt = new JdbcLookupBolt(analyzeConnectionProvider, sqlFindProps, jdbcLookupMapper);
        //3.3.2，写入platform_properties
		ArangoMapper arangoMapper = new SimpleArangoMapper(prop_fields);
		ArangoInsertBolt insertPropsBolt = new ArangoInsertBolt(props,"sea","platform_properties",arangoMapper);

        //装配topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("check_categoryId_spout", propertySpout, 1);
        builder.setBolt("find_mapping_categoryId", arangoLookupBolt, 1).shuffleGrouping("check_categoryId_spout");
        //更新my_stuff meta.category
        builder.setBolt("update_meta_category", changeCategoryBolt, 1).shuffleGrouping("find_mapping_categoryId");
        //更新分析库categoryId
        builder.setBolt("update_mapping_categoryId_property", jdbcUpdatePropertyBolt, 1).shuffleGrouping("find_mapping_categoryId");
        builder.setBolt("update_mapping_categoryId_value", jdbcUpdateValueBolt, 1).shuffleGrouping("find_mapping_categoryId");
        builder.setBolt("query_props_by_category", jdbcFindPropsBolt, 1).shuffleGrouping("find_mapping_categoryId");
        builder.setBolt("insert_platform_props", insertPropsBolt, 1).shuffleGrouping("query_props_by_category");
        return builder.createTopology();
    }
}
