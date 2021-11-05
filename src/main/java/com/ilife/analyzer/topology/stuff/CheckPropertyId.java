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
 * 
 * 将已经建立映射的属性ID补充到分析库property 及 value表
 * 
 * 处理逻辑：
 * 1，查询分析库 property 表内，获取所有propertyId为空的记录
 * 2，根据source、property名称从platform_properties内查找与标准类目的映射记录
 * 3.1 将映射信息更新到 property 表
 * 3.2 将映射信息更新到 value 表
 *
 */
public class CheckPropertyId extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new CheckPropertyId().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
    		//1，获取id为空的property记录，将匹配填写propertyId
    		PropertyIdSpout propertySpout = new PropertyIdSpout(analyzeConnectionProvider);
    		
    		//2，直接从platform_properties内读取映射记录，根据platform、property名称（props.xxx的xxx部分）匹配，得到property的mappingId
            String aql = "FOR doc in platform_properties filter doc.source==@platform and doc.name==@propName limit 1 return  {mappingId:doc.mappingId,mappingName:doc.mappingName,platform:doc.source,name:doc.name}";
            SimpleQueryFilterCreator queryCreator = new SimpleQueryFilterCreator().withField("platform","propName");
            String[] mapping_fields = {"mappingId","mappingName","platform","name"};//映射的标准属性Id、映射的标准属性名称、来源平台、原始属性名称
            ArangoLookupMapper mapper = new SimpleArangoLookupMapper(mapping_fields);
            ArangoLookupBolt arangoLookupBolt = new ArangoLookupBolt(props,"sea",aql,queryCreator,mapper);
            
            //3.1，将propertyId更新到property记录
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("mappingId", Types.VARCHAR),
            		//new Column("mappingName", Types.VARCHAR),
            		new Column("platform", Types.VARCHAR),
            		new Column("name", Types.VARCHAR));
            JdbcMapper updatePropMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdatePropBolt = new JdbcInsertBolt(analyzeConnectionProvider, updatePropMapper)
                    .withInsertQuery("update property set propertyId=?,status='ready',modifiedOn=now() where platform=? and property=?");
            	
            //3.2，将propertyId更新到value记录
            List<Column> valueSchemaColumns = Lists.newArrayList(
            		new Column("mappingId", Types.VARCHAR),
            		//new Column("mappingName", Types.VARCHAR),
            		new Column("platform", Types.VARCHAR),
            		new Column("name", Types.VARCHAR));
            JdbcMapper updateValueMapper = new SimpleJdbcMapper(valueSchemaColumns);
            JdbcInsertBolt jdbcUpdateValueBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateValueMapper)
                    .withInsertQuery("update `value` set propertyId=?,modifiedOn=now() where platform=? and property=?");

            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("check_propertyId_spout", propertySpout, 1);
	        builder.setBolt("find_prop_mapping_bolt", arangoLookupBolt, 1).shuffleGrouping("check_propertyId_spout");
	        builder.setBolt("update_property_with_propertyId", jdbcUpdatePropBolt, 1).shuffleGrouping("find_prop_mapping_bolt");
	        builder.setBolt("update_value_with_propertyId", jdbcUpdateValueBolt, 1).shuffleGrouping("find_prop_mapping_bolt");
	        return builder.createTopology();
	    }
}
