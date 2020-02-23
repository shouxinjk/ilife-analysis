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
 * 获取待结算broker列表。
 * 查询运行日期2天前的所有已清算记录，并且通过unique获得相应的broker列表。
 * 发送处理前需要将相应的记录状态设置为settling
 * @author alexchew
 *
 */
public class ClearedSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(ClearedSpout.class);
    
    public ClearedSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,connectionProvider);
    }
    
    public ClearedSpout(ConnectionProvider connectionProvider,ConnectionProvider bizConnectionProvider) {
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
    		//从清分库查询待结算列表，并进行分项汇总。得到beneficiary、type、date、sum值。
    		//注意：由于先汇总后更新状态，可能对于某些locked记录出现错误：在汇总时未计算，但在更新状态后被认为已计算。每次仅取一条汇总记录，减少差错可能性
    		//注意：此处存在性能问题：每次会对所有数据进行分组
        String sql = "SELECT a.beneficiary as beneficiary,a.scheme_item_id,b.beneficiary as type,date(a.create_date) as date,sum(amount_profit) as amount "
        		+ "FROM mod_clearing a left join mod_profit_share_item b on b.id=a.scheme_item_id "
        		+ "where a.status_clear='cleared' and a.status_settle='pending' and a.create_date<(CURRENT_DATE+1) "//注意：实际中使用当前日期-1计算
        		+ "group by a.beneficiary,date(a.create_date),b.beneficiary "
        		+ "limit 10";
        logger.debug("try to query pending cleared data.[SQL]"+sql);
        List<List<Column>> result = jdbcClient.select(sql,queryParams);
        if (result != null && result.size() != 0) {//如果有则直接发射
            for (List<Column> row : result) {
                Values values = new Values();
                String beneficiary="null";
                String date="null";
                String profit_scheme_item_id="null";
                for(Column column : row) {
                    values.add(column.getVal());
                    if("beneficiary".equalsIgnoreCase(column.getColumnName())) {
                    		beneficiary = column.getVal().toString();
                    }else if("scheme_item_id".equalsIgnoreCase(column.getColumnName())) {
	                		profit_scheme_item_id = column.getVal().toString();
		            }else if("date".equalsIgnoreCase(column.getColumnName())) {
	                		date = column.getVal().toString();
		            }
                }
                //更改订单为临时状态：clearing
                jdbcClient.executeSql("update mod_clearing set status_settle='settling',update_date=now() where beneficiary='__beneficiary'and scheme_item_id='_scheme_item_id' and status_clear='cleared' and status_settle='pending' and date(create_date)=date('__date')"
                		.replace("__beneficiary", beneficiary)
                		.replace("_scheme_item_id", profit_scheme_item_id)
                		.replace("__date", date));
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
        declarer.declare(new Fields("beneficiary","scheme_item_id","type","date","amount"));
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
