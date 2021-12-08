package com.ilife.analyzer.topology.stuff;

import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import com.ilife.analyzer.bolt.stuff.CreateEvaluateTaskBolt;
import com.ilife.analyzer.bolt.stuff.CreateMeasureTaskBolt;
import com.ilife.analyzer.bolt.stuff.InsertPropertyBolt;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 从采集库加载数据并分别写入分析库（包括存储与key-value库），主要逻辑如下：
 * 1，从arangodb读取状态为pending的初始数据，读取后即更新状态为ready
 * 2.1，组装默认数据后写入分析arangodb数据库
 * 2.2.1，按照属性打散数据记录写入key-value数据库
 * 2.2.2，将数值写入标注数据库
 * 
 * 拓扑图：
 * 采集库-----Arango分析库（保持原始结构）
 *        +--JSON解析为键值对-----写入属性库（category、property、itemKey、value）
 *         |                  +--写入标注库（category、property、value）
 *         +--建立客观评价任务（measure、property、weight、priority）
 *         +--建立主观评价任务（evaluation、dimension、weight、priority）
 */
public class Load extends AbstractTopology {
	    String arango_harvest = "sea";//采集库，存放原始采集数据
//	    String arango_analyze = "forge";//分析库，存放分析结果
	    
	    public static void main(String[] args) throws Exception {
	        new Load().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    		//0准备更新时间
	    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	    		//1，ArangoSpout：从arangodb读取状态为pending的初始数据，读取后即更新状态为ready
	    		String query = "FOR doc in my_stuff "
	    				+ "filter doc.status.load == 'pending' and doc.status.classify == 'ready' and doc.meta.category !=null "
	    				+ "update doc with { status:{load: 'ready'},timestamp:{load:'"+sdf.format(new Date())+"'},category:CONCAT_SEPARATOR(' ',doc.category) } in my_stuff "
	    				+ "limit 10 "
	    				+ "return NEW";
	    		String[] fields = {"_key","_doc","category","categoryId","source","meta.category"};
	    		ArangoSpout arangoSpout = new ArangoSpout(props,arango_harvest)
	    				.withQuery(query)
	    				.withFields(fields);
	   
	    		//2.1，ArangoInsert：组装默认数据后写入分析arangodb数据库
	    		/*
	    		String[] insertFields = {"_key","_doc"};
	    		ArangoMapper mapper = new SimpleArangoMapper(insertFields);
	    		ArangoInsertBolt insertBolt = new ArangoInsertBolt(props,arango_analyze,"stuff",mapper);
	    		//**/
	    		//2.2，按照属性打散数据记录写入key-value数据库
	    		//2.2.1，将json打散为行列数据
	    		String[] infields = {"_doc","category","categoryId","_key","source","meta.category"};//categoryId为原始类目编码
	    		String[] outfields = {"property","value","category","categoryId","itemKey","source","meta.category"};
	    		JsonParseBolt jsonParser = new JsonParseBolt(infields,outfields);//从 _doc字段读取json字符串，输出key、value字段; 并且附加其他字段如category,itemKey
	    		
	    		//2.2.2，行列数据写入关系数据库:category,property(key),value,status,modifiedOn。用于归一化任务。
            List<Column> columns = Lists.newArrayList(
            		new Column("property", Types.VARCHAR),
            		new Column("value", Types.VARCHAR),
            		new Column("category", Types.VARCHAR),
            		new Column("meta.category", Types.VARCHAR),
            		new Column("source", Types.VARCHAR),
            		new Column("itemKey", Types.VARCHAR));
            JdbcMapper jdbcMapper = new SimpleJdbcMapper(columns);
            JdbcInsertBolt jdbcInsertPropertyBolt = new JdbcInsertBolt(analyzeConnectionProvider, jdbcMapper)
                    .withInsertQuery("insert ignore into property(property,value,category,categoryId,platform,itemKey,status,createdOn,modifiedOn) "
                    		+ "values (?,?,?,?,?,?,'pending',now(),now()) on duplicate key update revision=revision+1");//属性表唯一校验规则：category、property、itemKey、value
//            		.withQueryTimeoutSecs(30); 
            //2.2.3，将属性写入标注数据表：用于归一化任务，收集手动或自动数据标注
            List<Column> columns2 = Lists.newArrayList(
            		new Column("property", Types.VARCHAR),
            		new Column("value", Types.VARCHAR),
            		new Column("source", Types.VARCHAR),
            		new Column("category", Types.VARCHAR),
            		new Column("meta.category", Types.VARCHAR));
            JdbcMapper jdbcMapper2 = new SimpleJdbcMapper(columns2);
            JdbcInsertBolt jdbcInsertValueBolt = new JdbcInsertBolt(analyzeConnectionProvider, jdbcMapper2)
                    .withInsertQuery("insert ignore into `value`(property,value,platform,category,categoryId,status,revision,createdOn,modifiedOn) "
                    		+ "values (?,?,?,?,?,'pending',1,now(),now())");//标注数据表唯一性校验规则：category、property、value。pending状态表示尚未经过标注，只是默认值  
//                    .withQueryTimeoutSecs(30);          
           
            //2.2.4，写入客观评价任务表：
            //写入measure-property：维度和属性关系。读取对应category的dimension-measure定义，写入分析库
            //写入measure：维度和维度关系。读取对应category的dimension-dimension定义，写入分析库            
            CreateMeasureTaskBolt createMeasureTaskBolt = new CreateMeasureTaskBolt(businessConnectionProvider,analyzeConnectionProvider);
            
            //2.2.5，写入主观评价任务表：
            //写入evalute-measure：主观维度和客观维度关系。读取对应category的evaluate-measure定义，写入分析库
            //写入evaluate：维度和维度关系。读取对应category的dimension-dimension定义，写入分析库            
            CreateEvaluateTaskBolt createEvaluateTaskBolt = new CreateEvaluateTaskBolt(businessConnectionProvider,analyzeConnectionProvider);

            
            //2.2.6，将类目下的属性清单写入platform_properties等待标注：           
            InsertPropertyBolt insertPropertyMappingBolt = new InsertPropertyBolt(props,"sea",businessConnectionProvider,analyzeConnectionProvider);
            
            
            //构建Topology
            String spout = "load-doc";
            String parseBolt = "parse-json";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(spout, arangoSpout, 1);
//	        builder.setBolt(saveToArangoBolt, insertBolt, 1).shuffleGrouping(spout);
	        builder.setBolt(parseBolt, jsonParser, 1).shuffleGrouping(spout);
	        builder.setBolt("insert-property", jdbcInsertPropertyBolt, 5).shuffleGrouping(parseBolt);
	        builder.setBolt("insert-value", jdbcInsertValueBolt, 5).shuffleGrouping(parseBolt);
	        builder.setBolt("create-measure-tasks", createMeasureTaskBolt, 1).shuffleGrouping(spout);
	        builder.setBolt("create-evaluate-tasks", createEvaluateTaskBolt, 1).shuffleGrouping(spout);
	        builder.setBolt("insert-property-mapping", insertPropertyMappingBolt, 1).shuffleGrouping(parseBolt);
	        return builder.createTopology();
	    }
}
