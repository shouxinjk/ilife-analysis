package com.ilife.analyzer.spout.person;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.google.common.collect.Lists;

import java.sql.Types;
import java.util.*;

/**
 * 读取待重新建立Filter记录。
 * 如果记录为空，则更新所有记录状态为pending，并升级版本。
 * 如果有待处理记录，则直接发射供后续bolt进行归一化操作
 * @author alexchew
 *
 */
public class FilterSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(FilterSpout.class);
    
    public FilterSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,"pending");
    }
    
    public FilterSpout(ConnectionProvider connectionProvider,String status) {
        this.isDistributed = true;
        this.connectionProvider = connectionProvider;
        this.queryParams = new ArrayList<Column>();
        queryParams.add(new Column("status", status, Types.VARCHAR));
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }
        connectionProvider.prepare();
        this.jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
    }

    public void close(){
	    	//TODO need to check if other bolts will be impacted
	    	connectionProvider.cleanup();
    }

    public void nextTuple() {
        String sql = "select userKey from user_filter where status=? limit 10";
        logger.debug("try to query pending user filters.[SQL]"+sql+"[query]"+queryParams);
        List<List<Column>> result = jdbcClient.select(sql,queryParams);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
            		logger.debug("got result.[row]"+row);
            		String userKey = "";
                Values values = new Values();
                for(Column column : row) {
                    values.add(column.getVal());
                    if("userKey".equalsIgnoreCase(column.getColumnName())) {
                    		userKey = column.getVal().toString();
                    }
                }
                this.collector.emit(values);
                
                //将记录状态更新为ready
                sql = "update user_filter set status='ready',revision=revision+1,modifiedOn=now() where userKey='_USERKEY'";
                sql = sql.replace("_USERKEY", userKey);
	        		logger.debug("try to update filter status.[SQL]"+sql);
	            jdbcClient.executeSql(sql); 
            }
        }else {//如果没有待处理记录，则将所有记录状态更新为pending，并且版本+1
        		sql = "update user_filter set status='pending',revision=revision+1,modifiedOn=now()";
        		logger.debug("try to update filter revision.[SQL]"+sql);
            jdbcClient.executeSql(sql); 
        }
        Thread.yield();
    }


    public void ack(Object msgId) {
    	//TODO here we should update ta_user.lastEvaluatedOn
    }

    public void fail(Object msgId) {
    	//do nothing
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userKey"));
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
