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
 * 查询得到propertyID为空的proeprty记录
 * @author alexchew
 *
 */
public class PropertyIdSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(PropertyIdSpout.class);
    
    public PropertyIdSpout(ConnectionProvider connectionProvider) {
        this.isDistributed = true;
        this.connectionProvider = connectionProvider;
        this.queryParams = new ArrayList<Column>();
        //queryParams.add(new Column("status", status, Types.VARCHAR));
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
        String sql = "select property as PropertyName from user_property where propertyId is null limit 50";
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
        }else {//do nothing
        		logger.info("none record has propertyId=null.");
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
        declarer.declare(new Fields("propertyName"));
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
