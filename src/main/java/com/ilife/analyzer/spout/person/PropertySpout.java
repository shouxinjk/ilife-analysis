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
 * 读取待归一化记录。
 * 如果归一化记录为空，则更新所有记录状态为pending，并升级版本。
 * 如果有待处理归一化记录，则直接发射供后续bolt进行归一化操作
 * @author alexchew
 *
 */
public class PropertySpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(PropertySpout.class);
    
    public PropertySpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,"pending");
    }
    
    public PropertySpout(ConnectionProvider connectionProvider,String status) {
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
    		//SQL:select category,property,value from property where status='pending' limit 50
        String sql = "select property,value from user_property where status=? limit 50";
        logger.debug("try to query candidate properties.[SQL]"+sql+"[query]"+queryParams);
        List<List<Column>> result = jdbcClient.select(sql,queryParams);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
            		logger.debug("got result.[row]"+row);
                Values values = new Values();
                for(Column column : row) {
                    values.add(column.getVal());
                }
                this.collector.emit(values);
            }
        }else {//如果没有待处理记录，则将所有记录状态更新为pending，并且版本+1
        		sql = "update user_property set status='pending',revision=revision+1";
        		logger.debug("try to update property revision.[SQL]"+sql);
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
        declarer.declare(new Fields("property","value"));
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
