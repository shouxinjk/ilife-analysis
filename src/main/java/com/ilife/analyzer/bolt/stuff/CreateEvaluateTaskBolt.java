
package com.ilife.analyzer.bolt.stuff;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.arangodb.bolt.AbstractArangoBolt;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 建立客观评价任务，包括6个类别：
 * 1，效益评价：类型为perform，有多条，有多级，按优先级排序。优先级800+,叶子节点900
 * 2，成本评价：类型为cost，有多条，有多级，按优先级排序。优先级700+，叶子节点800
 * 3，约束评价：类型为filter，有多条，仅一级，优先级600+
 * 4，需求满足度评价：类型为satisfy，仅一条，优先级500
 * 5，情境满足度评价：类型为context，仅一条，优先级400
 * 6，偏好评价：类型为style，仅一条，优先级300
 * 
 * 任务类型包括两类：
 * 1，对于效益、成本、约束，需要建立evaluation-measure任务，以及evaluation-evaluation任务
 * 2，对于需求满足、情境满足、偏好，仅建立evaluation任务，根据定义的算法以当前item为参数进行计算
 * 
 */
public class CreateEvaluateTaskBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(CreateEvaluateTaskBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    public List<Column> queryParams;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"_key","category"};//输入字段包含itemkey和category
    String[] outfields = {"itemKey","category"};//输出字段包含itemKey和Category，便于创建后续分析任务
    
    Map<String,Integer> typePriority = new HashMap<String,Integer>();
    
    public CreateEvaluateTaskBolt(ConnectionProvider connectionProvider) {
		this(connectionProvider,connectionProvider);
    }
    
    public CreateEvaluateTaskBolt(ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
    		this.connectionProviderBiz = connectionProviderBiz;
    		this.connectionProviderAnalyze = connectionProviderAnalyze;
    		
        this.queryParams = new ArrayList<Column>();
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        connectionProviderBiz.prepare();
        connectionProviderAnalyze.prepare();
        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }

        this.jdbcClientBiz = new JdbcClient(connectionProviderBiz, queryTimeoutSecs);
        this.jdbcClientAnalyze = new JdbcClient(connectionProviderAnalyze, queryTimeoutSecs);
        
        typePriority.put("perform", 800);
        typePriority.put("cost", 700);
        typePriority.put("filter", 600);
        typePriority.put("satisfy", 500);
        typePriority.put("context", 400);
        typePriority.put("style", 300);
    }

    public void execute(Tuple tuple) {
    	 	String category = tuple.getStringByField("category");//获取category字段
    	 	queryParams.clear();
        queryParams.add(new Column("category", category, Types.VARCHAR));
        
 	    	//查询evaluation-evaluation记录
 		//1，查询业务库得到evaluation-evaluation记录
     	//select mod.parent_id as parent,mod.id as evaluation, mod.weight as weight,mod.type as type,mod.parent_ids as depth from mod_category cat,mod_item_evaluation mod where cat.name=? and cat.id=mod.category
     		//2，写入分析库
     	//insert ignore into evaluation (itemKey,parent,evaluation,weight,type,script,status,priority,revision,createdOn,modifiedOn) values()
    		String sqlQuery = "select m.parent_id as parent,m.id as evaluation, m.weight as weight,m.type as type,m.script as script,m.parent_ids as depth,cat.id as category from mod_item_category cat,mod_item_evaluation m where cat.name=? and cat.id=m.category";
         logger.debug("try to query pending measure-dimension.[SQL]"+sqlQuery);
         String sqlInsert = "insert ignore into evaluation (itemKey,parent,evaluation,weight,type,script,category,status,priority,revision,createdOn,modifiedOn) values(?,?,?,?,?,?,?,'pending',?,1,now(),now())";
         List<List<Column>> items = new ArrayList<List<Column>>();
         List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
         if (result != null && result.size() != 0) {
             for (List<Column> row : result) {
             		List<Column> item = new ArrayList<Column>();
             		item.add(new Column("itemKey",tuple.getValueByField("_key"),Types.VARCHAR));//itemKey
             		int priorityBase = 400;//根据type动态修改
             		int priorityDepth = 0;
             		for(Column column:row) {//dimension，measure，weight
             			if("depth".equalsIgnoreCase(column.getColumnName())) {
             				String[] depth = column.getVal().toString().split(",");
             				priorityDepth = depth.length;//按照层级，叶子节点优先级更高
             			}else {//作为插入参数
             				item.add(new Column(column.getColumnName(), column.getVal(), column.getSqlType()));
             			}
             			//根据type设置优先级
             			if("type".equalsIgnoreCase(column.getColumnName())) {
             				String type = column.getVal().toString();
             				priorityBase = typePriority.get(type)==null?0:typePriority.get(type);
             			}
             		}
             		int priority = priorityBase+priorityDepth;
             		item.add(new Column("priority",priority,Types.INTEGER));
             		items.add(item);
             }
 	    		//写入分析库
            logger.info("try to insert pending evaluate-dimension tasks.[SQL]"+sqlInsert+"[items]"+items);
 	    		jdbcClientAnalyze.executeInsertQuery(sqlInsert, items);	    		
         }else {//没有配置measure-measure的情况：do nothing
         		logger.debug("no more evaluation-evaluation tasks");
         }   
        
        //查询evaluate-measure记录
    		//1，查询业务库得到evaluate-measure记录
    	//select int.evaluation_id as evaluation,int.dimension_id as dimension,int.weight as weight,mod.type as type from mod_category cat,int_item_evaluation_dimension int,mod_item_evaluation mod where cat.name=? and cat.id=int.category and int.evaluation=mod.id
    		//2，写入分析库
    	//insert ignore into evaluation_measure (itemKey,evaluation,dimension,weight,type,script,status,priority,revision,createdOn,modifiedOn) values()
        sqlQuery = "select inter.evaluation_id as evaluation,inter.dimension_id as dimension,inter.weight as weight,m.type as type,m.script as script,cat.id as category from mod_item_category cat,int_item_evaluation_dimension inter,mod_item_evaluation m where cat.name=? and cat.id=inter.category and inter.evaluation_id=m.id";
        logger.debug("try to query pending evaluation-measure.[SQL]"+sqlQuery);
        sqlInsert = "insert ignore into evaluation_measure (itemKey,evaluation,dimension,weight,type,script,category,status,priority,revision,createdOn,modifiedOn) values(?,?,?,?,?,?,?,'pending',?,1,now(),now())";
        items = new ArrayList<List<Column>>();
        result = jdbcClientBiz.select(sqlQuery,queryParams);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
            		List<Column> item = new ArrayList<Column>();
            		item.add(new Column("itemKey",tuple.getValueByField("_key"),Types.VARCHAR));//itemKey
            		int priority = 900;
            		String evalute = "";
            		String type="";
            		for(Column column:row) {//evaluation,dimension,weight,type
        				item.add(new Column(column.getColumnName(), column.getVal(), column.getSqlType()));
            			//根据type设置优先级
            			if("type".equalsIgnoreCase(column.getColumnName())) {
            				type = column.getVal().toString();
            				priority = typePriority.get(type)==null?0:(typePriority.get(type)+900);//根据基础priority计算得到，所有叶子节点都列为最高优先级，注意，这一优先级需要更新到evaluate任务
            			}
            			if("evaluation".equalsIgnoreCase(column.getColumnName())) {
            				evalute = column.getVal().toString();
            			}
            		}
            		item.add(new Column("priority",priority,Types.INTEGER));//priority:叶子节点优先级手动设置为900
            		items.add(item);
            		//更新对应evaluate任务优先级
            		String updateSql = "update evaluation set priority='_PRIORITY' where itemKey='_ITEMKEY' and evaluation='_EVALUATE' and type='_TYPE'"
            				.replace("_PRIORITY", ""+priority)
            				.replace("_ITEMKEY", tuple.getValueByField("_key").toString())
            				.replace("_EVALUATE", evalute)
            				.replace("_TYPE", type);
            		jdbcClientAnalyze.executeSql(updateSql);
            }
	    		//写入分析库
            logger.info("try to insert pending evaluation-measure tasks.[SQL]"+sqlInsert+"[items]"+items);
	    		jdbcClientAnalyze.executeInsertQuery(sqlInsert, items);
        }else {//没有配置evaluation-measure的情况：do nothing
	    		logger.debug("No pending evaluation-measure tasks");
	    }
 
        //将itemKey、category向后传递
    		try {
	    		Values values = new Values();
	    		for(String field:inputFields) {
	    			values.add(tuple.getValueByField(field));
	    		}
	    		this.collector.emit(values);
	    } catch (Exception e) {
	        this.collector.reportError(e);
	    }
	    Thread.yield();
    }

    @Override
    public void cleanup() {
    		connectionProviderBiz.cleanup();
    		connectionProviderAnalyze.cleanup();
    } 
    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
