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
 * 根据已经建立类目映射的属性，检查继承属性，并建立映射关系。
 * 
 * 1，查询分析库 property 表内，获取拥有categoryId且propertyId为空的记录
 * 2，根据categoryId查询业务库，获取该类目的所有上级节点ID列表
 * 3，根据类目上级节点ID列表查询property，根据propertyKey进行匹配，得到相应的propertyId
 * 4.1 将映射信息更新到 property 表
 * 4.2 将映射信息更新到 value 表
 *
 */
public class CheckPropertyInherit extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new CheckPropertyInherit().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
    		//1，获取id为空的property记录，将匹配填写propertyId
    		PropertyIdSpout propertySpout = new PropertyIdSpout(analyzeConnectionProvider,true);//表示检查所有propertyId为空的记录
    		
    		//2，根据categoryId获取对应类目的所有上级类目ID列表。返回字符串数组。
    		//select concat('"',replace(substring(parent_ids,1,length(parent_ids)-1),',','","'),'"') as categoryIds from mod_item_category where id='228887d456ac4b91bb23c0299238dc95'
            String sqlFindCategoryParents = "select ? as platform,? as propKey,substring(parent_ids,1,length(parent_ids)-1) as categoryIds "
            		+ "from mod_item_category where id=?";
            List<Column> queryParamColumns = Lists.newArrayList(
            		new Column("platform", Types.VARCHAR),//来源信息作为更新标志
            		new Column("propName", Types.VARCHAR),
            		new Column("categoryId", Types.VARCHAR));
            Fields outputFields = new Fields("platform","propKey","categoryIds");
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindParentCategoriesBolt = new JdbcLookupBolt(businessConnectionProvider, sqlFindCategoryParents, jdbcLookupMapper);

            //3，查询上级目录的继承属性:仅获得最近的一条即可
            //SELECT c.id as categoryId,m.id as propertyId,c.name as category,m.name as propertyName,m.property as propertyKey FROM mod_measure m left join mod_item_category  c on m.category = c.id  where c.id in("0","1","d1668f8b3c9748cd806462a45651827b","228887d456ac4b91bb23c0299238dc95") and m.property='props.departure' order by c.create_date desc limit 1
            String sqlFindInheritProps = "SELECT ? as platform,m.id as propertyId,m.property as property "
            		+ "FROM mod_measure m "
            		+ "left join mod_item_category  c "
            		+ "on m.category = c.id  where FIND_IN_SET(c.id,?) and m.property=? order by c.create_date desc limit 1";
            List<Column> inheritPropParams = Lists.newArrayList(
            		new Column("platform", Types.VARCHAR),//来源信息作为更新标志
            		new Column("categoryIds", Types.VARCHAR),
            		new Column("propKey", Types.VARCHAR));
            Fields inheritPropFields = new Fields("platform","propertyId","property");
            JdbcLookupMapper inheritPropsLookupMapper = new SimpleJdbcLookupMapper(inheritPropFields, inheritPropParams);
            JdbcLookupBolt jdbcFindInheritPropsBolt = new JdbcLookupBolt(businessConnectionProvider, sqlFindInheritProps, inheritPropsLookupMapper);
            
            //4.1，将propertyId更新到property记录
            List<Column> propertySchemaColumns = Lists.newArrayList(
            		new Column("propertyId", Types.VARCHAR),
            		new Column("platform", Types.VARCHAR),
            		new Column("property", Types.VARCHAR));
            JdbcMapper updatePropMapper = new SimpleJdbcMapper(propertySchemaColumns);
            JdbcInsertBolt jdbcUpdatePropBolt = new JdbcInsertBolt(analyzeConnectionProvider, updatePropMapper)
                    .withInsertQuery("update property set propertyId=?,status='ready',isInherit=1,modifiedOn=now() where platform=? and property=?");
            	
            //4.2，将propertyId更新到value记录
            List<Column> valueSchemaColumns = Lists.newArrayList(
            		new Column("propertyId", Types.VARCHAR),
            		new Column("platform", Types.VARCHAR),
            		new Column("property", Types.VARCHAR));
            JdbcMapper updateValueMapper = new SimpleJdbcMapper(valueSchemaColumns);
            JdbcInsertBolt jdbcUpdateValueBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateValueMapper)
                    .withInsertQuery("update `value` set propertyId=?,modifiedOn=now() where platform=? and property=?");

            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("check_inherit_propertyId_spout", propertySpout, 1);
	        builder.setBolt("find_parent_category_bolt", jdbcFindParentCategoriesBolt, 1).shuffleGrouping("check_inherit_propertyId_spout");
	        builder.setBolt("find_inherit_props_bolt", jdbcFindInheritPropsBolt, 1).shuffleGrouping("find_parent_category_bolt");
	        builder.setBolt("update_property_with_propertyId", jdbcUpdatePropBolt, 1).shuffleGrouping("find_inherit_props_bolt");
	        builder.setBolt("update_value_with_propertyId", jdbcUpdateValueBolt, 1).shuffleGrouping("find_inherit_props_bolt");
	        return builder.createTopology();
	    }
}
