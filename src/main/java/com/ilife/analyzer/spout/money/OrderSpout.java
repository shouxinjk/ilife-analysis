package com.ilife.analyzer.spout.money;

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
 * 获取待清算order列表。
 * 输入：订单状态
 * 输出：订单记录列表
 * @author alexchew
 *
 */
public class OrderSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(OrderSpout.class);
    
    public OrderSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,connectionProvider);
    }
    
    public OrderSpout(ConnectionProvider connectionProvider,ConnectionProvider bizConnectionProvider) {
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
    		//从业务库查询待处理订单列表
        String sql = "select a.id,a.item,a.amount,a.order_time,a.platform,a.trace_code,a.commission_settlement as commission,b.broker_id as broker_id from mod_order a LEFT JOIN mod_trace_code b ON a.trace_code = b.code and a.platform=b.platform where a.status='pending' limit 10";
        logger.debug("try to query pending orders.[SQL]"+sql);
        List<List<Column>> result = jdbcClient.select(sql,queryParams);
        if (result != null && result.size() != 0) {//如果有则直接发射
            for (List<Column> row : result) {
                Values values = new Values();
                String id="null";
                for(Column column : row) {
                    values.add(column.getVal());
                    if("id".equalsIgnoreCase(column.getColumnName())) {
                    		id = column.getVal().toString();
                    }
                }
                //更改订单为临时状态：clearing
                jdbcClient.executeSql("update mod_order set status='clearing' where id='__id'".replace("__id", id));
                this.collector.emit(values);
            }
        }else {//do nothing
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
        declarer.declare(new Fields("id","item","amount","order_time","platform","trace_code","commission","broker_id"));
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
