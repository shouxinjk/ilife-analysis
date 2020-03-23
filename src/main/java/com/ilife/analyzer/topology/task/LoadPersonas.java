package com.ilife.analyzer.topology.task;

import java.sql.Types;
import java.util.List;

import org.apache.flink.storm.api.FlinkTopology;
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
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * @author alexchew
 * 加载用户数据并完成个性化查询，得到候选列表
 * 1，从arangodb获取 status=pending 或 status=ready && recommend=false 状态用户数据，包括id、filter
 * 2，将用户数据写入分析库，建立自动推荐任务，默认状态为pending
 * 3，
 * 拓扑图：
 * Arango用户
 *        \--构建自动推荐任务-----写入推荐任务库（userId、tags、lastModified、lastCalculated、status）
 *                           
 */
public class LoadPersonas extends AbstractTopology {
	    String arango_harvest = "sea";//采集库，存放原始采集数据
	    String arango_analyze = "forge";//分析库，存放分析结果
	    
	    public static void main(String[] args) throws Exception {
	        new LoadPersonas().execute(args);
	    }

	    @Override
	    public FlinkTopology getTopology() {
	    		//1，ArangoSpout：从arangodb读取状态为pending的初始数据，读取后即更新状态为ready
	    		String query = "FOR doc in persona_personas filter doc.isRecommend == null or doc.status='pending' update doc with { isRecommend: true } in persona_personas limit 10 return OLD";
	    		String[] fields = {"_key","query","tags"};//query是个性化查询
	    		ArangoSpout arangoSpout = new ArangoSpout(props,arango_harvest)
	    				.withQuery(query)
	    				.withFields(fields);
	    		
	    		//2.1，写入待推荐用户表
            List<Column> columns = Lists.newArrayList(
            		new Column("userId", Types.VARCHAR),
            		new Column("query", Types.VARCHAR),
            		new Column("tags", Types.VARCHAR));
            JdbcMapper jdbcMapper = new SimpleJdbcMapper(columns);
            JdbcInsertBolt jdbcInsertRecommdationTaskBolt = new JdbcInsertBolt(analyzeConnectionProvider, jdbcMapper)
                    .withInsertQuery("insert ignore into recommendationTask(id,query,type,tags,status,modifiedOn,ProcessedOn) "
                    		+ "values (?,?,'persona',?,'pending',now(),now()) on duplicate key update revision=revision+1");//属性表唯一校验规则：type、userId

            //构建Topology
            String spout = "loadusers_spout_load_from_harvest_arango";
            String insertTaskBolt = "loadusers_bolt_insert_tasks";
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout(spout, arangoSpout, 1);
	        builder.setBolt(insertTaskBolt, jdbcInsertRecommdationTaskBolt, 5).shuffleGrouping(spout);
	        return FlinkTopology.createTopology(builder);
	    }
}
