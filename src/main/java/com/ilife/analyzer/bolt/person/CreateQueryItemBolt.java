
package com.ilife.analyzer.bolt.person;
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
 * 建立Filter条目，用于构建个性化查询。字段包括：
 * userKey：用户ID
 * category：过滤类别，Must/Must Not/Should/Filter。否则不做处理
 * type：指过滤类型。term/match。当前固定为match
 * field：指定的过滤字段。如果为空则不处理。注意来源字段为“type”，由用户自行指定。注意：由于存在多个item中对同一个字段进行过滤，采用子字段定义。如brand::tags，表示过滤brand信息，但过滤是通过tags字段完成的
 * text：默认为null。表示不予处理
 * 
 */
public class CreateQueryItemBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(CreateQueryItemBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    public List<Column> queryParams;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"_key"};//输入字段包含itemkey
    String[] outfields = {"userKey"};//输出字段包含userKey，便于创建后续分析任务
    
    public CreateQueryItemBolt(ConnectionProvider connectionProvider) {
		this(connectionProvider,connectionProvider);
    }
    
    public CreateQueryItemBolt(ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
    }

    public void execute(Tuple tuple) {
    	 	queryParams.clear();
    	 	
    		String sqlQuery = "select category as category,'match' as type, type as field from mod_user_evaluation";
         logger.debug("try to query pending user_query.[SQL]"+sqlQuery);
         String sqlInsert = "insert ignore into user_query (userKey,category,type,subType,field,text,status,revision,createdOn,modifiedOn) values(?,?,?,?,?,null,'pending',1,now(),now())";
         List<List<Column>> items = new ArrayList<List<Column>>();
         List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
         if (result != null && result.size() != 0) {
             for (List<Column> row : result) {
             		List<Column> item = new ArrayList<Column>();
             		item.add(new Column("userKey",tuple.getValueByField("_key"),Types.VARCHAR));//userKey
             		for(Column column:row) {
             			if("field".equalsIgnoreCase(column.getColumnName())) {
             				//对于field字段，需要处理是否有子类型定义。子类型为 brand::tags，表示实际field为tags
             				String[] fields = column.getVal().toString().split("::");
             				if(fields.length>1) {
             					item.add(new Column(column.getColumnName(), fields[0], column.getSqlType()));
             					item.add(new Column(column.getColumnName(), fields[1], column.getSqlType()));
             				}else {
             					item.add(new Column(column.getColumnName(), "-", column.getSqlType()));//subtype为空
             					item.add(new Column(column.getColumnName(), fields[0], column.getSqlType()));
             				}
             			}else {//直接添加参数
             				item.add(new Column(column.getColumnName(), column.getVal(), column.getSqlType()));
             			}
             		}
             		items.add(item);
             }
 	    		//写入分析库
            logger.info("try to insert pending user_query items.[SQL]"+sqlInsert+"[items]"+items);
 	    		jdbcClientAnalyze.executeInsertQuery(sqlInsert, items);	    		
         }else {//没有配置measure-measure的情况：do nothing
         		logger.debug("no more evaluation-evaluation tasks");
         }   
 
        //将userKey、category向后传递
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
