package com.ilife.analyzer.spout.stuff;

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
 * 读取待补充marked_value的自动标注记录。
 * 
 * SQL：
	SELECT p.id as id,p.category_id as categoryId,p.measure_id as propertyId,
	p.original_value as `value`,m.control_value as score 
	FROM ope_performance p 
	left join mod_measure m on p.measure_id=m.id 
	where m.auto_label_type='auto' 
	order by p.marked_value,p.update_date limit 100
 *
 */
public class PreNormAutoSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(PreNormAutoSpout.class);
    
    public PreNormAutoSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,"pending");
    }
    
    public PreNormAutoSpout(ConnectionProvider connectionProvider,String status) {
        this.isDistributed = true;
        this.connectionProvider = connectionProvider;
        this.queryParams = new ArrayList<Column>();
//        queryParams.add(new Column("status", status, Types.VARCHAR));
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
//	    	connectionProvider.cleanup();
    }

    public void nextTuple() {
    	//其中score为属性定义上的默认值。必须填值，避免数值字段无法有效转换时导致阻塞
        String sql = "SELECT p.id as id,p.category_id as categoryId,p.measure_id as propertyId,"
        		+ "p.original_value as `value`,m.default_score as score "
        		+ "FROM ope_performance p "
        		+ "left join mod_measure m on p.measure_id=m.id "
        		+ "where p.isReady=0 and m.auto_label_type='auto' "
        		+ "order by p.marked_value,p.update_date limit 100";
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
        }else {//如果没有待处理记录
        	//do nothing
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
        declarer.declare(new Fields("id","categoryId","propertyId","value","score"));
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
