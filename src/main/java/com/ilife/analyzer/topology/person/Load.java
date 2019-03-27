package com.ilife.analyzer.topology.person;

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
import com.ilife.analyzer.bolt.JsonParseBolt;
import com.ilife.analyzer.bolt.person.CreateEvaluateTaskBolt;
import com.ilife.analyzer.bolt.person.CreateMeasureTaskBolt;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 从原始库加载数据并分别写入分析库（包括存储与key-value库），主要逻辑如下：
 * 1，从arangodb读取状态为pending的初始数据，读取后即更新状态为ready
 * 2.1，组装默认数据后写入分析arangodb数据库
 * 2.2.1，按照属性打散数据记录写入key-value数据库
 * 2.2.2，将数值写入标注数据库
 * 
 * 拓扑图：
 * 采集库-----Arango分析库（保持原始结构）
 *        \--JSON解析为键值对-----写入属性库（property、userKey、value）
 *                           |--写入标注库（property、value）
 *                           |--建立客观评价任务（measure、property、weight、priority）
 *                           |--建立主观评价任务（evaluation、dimension、weight、priority）
 *                           |--查询所有分群--建立分群评价任务（userKey、hierarchyId、phaseId、personaId）
 *                           
 */
public class Load extends AbstractTopology {
	    String arango_harvest = "sea";//采集库，存放原始采集数据
	    String arango_analyze = "forge";//分析库，存放分析结果
	    
	    public static void main(String[] args) throws Exception {
	        new Load().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//1，ArangoSpout：从arangodb读取状态为pending的初始数据，读取后即更新状态为ready
	    		String query = "FOR doc in user_users filter doc.status == 'pending' update doc with { status: 'ready' } in user_users limit 10 return OLD";
	    		String[] fields = {"_key","_doc"};
	    		ArangoSpout arangoSpout = new ArangoSpout(props,arango_harvest)
	    				.withQuery(query)
	    				.withFields(fields);
	    		
	    		//2.1，ArangoInsert：组装默认数据后写入分析arangodb数据库
	    		String[] insertFields = {"_key","_doc"};
	    		ArangoMapper mapper = new SimpleArangoMapper(insertFields);
	    		ArangoInsertBolt insertBolt = new ArangoInsertBolt(props,arango_analyze,"person",mapper);
	    		
	    		//2.2，按照属性打散数据记录写入key-value数据库
	    		//2.2.1，将json打散为行列数据
	    		String[] infields = {"_doc","_key"};
	    		String[] outfields = {"property","value","userKey"};
	    		JsonParseBolt jsonParser = new JsonParseBolt(infields,outfields);//从 _doc字段读取json字符串，输出key、value字段; 并且附加其他字段如userKey
	    		
	    		//2.2.2，行列数据写入关系数据库:property(key),value,status,modifiedOn。用于归一化任务。
            List<Column> columns = Lists.newArrayList(
            		new Column("property", Types.VARCHAR),
            		new Column("value", Types.VARCHAR),
            		new Column("userKey", Types.VARCHAR));
            JdbcMapper jdbcMapper = new SimpleJdbcMapper(columns);
            JdbcInsertBolt jdbcInsertPropertyBolt = new JdbcInsertBolt(analyzeConnectionProvider, jdbcMapper)
                    .withInsertQuery("insert ignore into user_property(property,value,userKey,status,createdOn,modifiedOn) "
                    		+ "values (?,?,?,'pending',now(),now()) on duplicate key update revision=revision+1");//属性表唯一校验规则：property、userKey、value
            
            //2.2.3，将属性写入标注数据表：用于归一化任务，收集手动或自动数据标注
            List<Column> columns2 = Lists.newArrayList(
            		new Column("property", Types.VARCHAR),
            		new Column("value", Types.VARCHAR));
            JdbcMapper jdbcMapper2 = new SimpleJdbcMapper(columns2);
            JdbcInsertBolt jdbcInsertValueBolt = new JdbcInsertBolt(analyzeConnectionProvider, jdbcMapper2)
                    .withInsertQuery("insert ignore into user_value(property,value,status,revision,createdOn,modifiedOn) "
                    		+ "values (?,?,'pending',1,now(),now())");//标注数据表唯一性校验规则：property、value。pending状态表示尚未经过标注，只是默认值            
           
            //2.2.4，写入客观评价任务表：
            //写入measure-property：维度和属性关系。读取对应category的dimension-measure定义，写入分析库
            //写入measure：维度和维度关系。读取对应category的dimension-dimension定义，写入分析库            
            CreateMeasureTaskBolt createMeasureTaskBolt = new CreateMeasureTaskBolt(businessConnectionProvider,analyzeConnectionProvider);
            
            //2.2.5，写入主观评价任务表：
            //写入evalute-measure：主观维度和客观维度关系。读取对应category的evaluate-measure定义，写入分析库
            //写入evaluate：维度和维度关系。读取对应category的dimension-dimension定义，写入分析库            
            CreateEvaluateTaskBolt createEvaluateTaskBolt = new CreateEvaluateTaskBolt(businessConnectionProvider,analyzeConnectionProvider);
 
            //2.2.6，写入分群任务表：           
            //读取persona记录
	        String sql = "select ? as userKey,hierarchy_id as hierarchyId,phase_id as phaseId,id as personaId from mod_persona";
	        List<Column> queryParamColumns = Lists.newArrayList(
	        		new Column("_key", Types.VARCHAR));
	        String[] output_fields = {"userKey","hierarchyId","phaseId","personaId"};
	        Fields outputFields = new Fields(output_fields);
	        JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
	        JdbcLookupBolt jdbcFindPersonaBolt = new JdbcLookupBolt(businessConnectionProvider, sql, jdbcLookupMapper);
	        //将userKey、hierarchy、phase、persona写入分析库
	        List<Column> propertySchemaColumns = Lists.newArrayList(
	        		new Column("userKey", Types.VARCHAR),
	        		new Column("hierarchyId", Types.VARCHAR),
	        		new Column("phaseId", Types.VARCHAR),
	        		new Column("personaId", Types.VARCHAR));
	        JdbcMapper updateMapper = new SimpleJdbcMapper(propertySchemaColumns);
	        JdbcInsertBolt jdbcInsertPersonaBolt = new JdbcInsertBolt(analyzeConnectionProvider, updateMapper)
	                .withInsertQuery("insert ignore into user_persona(userKey,hierarchyId,phaseId,personaId,status,createdOn,modifiedOn) "
	                		+ "values (?,?,?,?,'pending',now(),now()) on duplicate key update revision=revision+1");//属性表唯一校验规则：userKey、hierarchyId、phaseId、personaId

            //构建Topology
            String spout = "load_user_spout_load_from_harvest_arango";
            String saveToArangoBolt = "load_user_bolt_save_to_analyze_arango";
            String parseBolt = "load_user_bolt_parse_json";
            String insertPropertyBolt = "load_user_bolt_insert_property";
            String insertValueBolt = "load_user_bolt_insert_value";
            String createMeasureTasksBolt = "load_user_bolt_create_measure_tasks";
            String createEvaluateTasksBolt = "load_user_bolt_create_evaluate_tasks";
            String findPersonaBolt = "load_user_bolt_find_all_persona";
            String createPersonaTasksBolt = "load_user_bolt_create_persona_tasks";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(spout, arangoSpout, 1);
	        builder.setBolt(saveToArangoBolt, insertBolt, 1).shuffleGrouping(spout);
	        builder.setBolt(parseBolt, jsonParser, 1).shuffleGrouping(spout);
	        builder.setBolt(findPersonaBolt, jdbcFindPersonaBolt, 1).shuffleGrouping(spout);
	        builder.setBolt(createPersonaTasksBolt, jdbcInsertPersonaBolt, 5).shuffleGrouping(findPersonaBolt);
	        builder.setBolt(insertPropertyBolt, jdbcInsertPropertyBolt, 5).shuffleGrouping(parseBolt);
	        builder.setBolt(insertValueBolt, jdbcInsertValueBolt, 5).shuffleGrouping(parseBolt);
	        builder.setBolt(createMeasureTasksBolt, createMeasureTaskBolt, 1).shuffleGrouping(spout);
	        builder.setBolt(createEvaluateTasksBolt, createEvaluateTaskBolt, 1).shuffleGrouping(spout);
	        return builder.createTopology();
	    }
}
