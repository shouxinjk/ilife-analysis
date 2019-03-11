package com.ilife.analyzer.spout;

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
 * 获取待处理evaluate-evaluate任务
 * 如果记录为空，则更新所有记录状态为pending，并升级版本。
 * 如果有待处理归一化记录，则直接发射供后续bolt进行归一化操作
 * @author alexchew
 *
 */
public class EvaluateDimensionSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> queryParams;
    
    private static final Logger logger = Logger.getLogger(EvaluateDimensionSpout.class);
    
    public EvaluateDimensionSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,connectionProvider);
    }
    
    public EvaluateDimensionSpout(ConnectionProvider connectionProvider,ConnectionProvider bizConnectionProvider) {
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
    		//从分析库里查询待处理任务:查询pending状态的非叶子节点: isFeature用于判定是否是顶级节点
        String sql = "select itemKey,evaluation,type,script,category,featured,itemKey as itemKey2,evaluation as evaluation2 from evaluation where status='pending' and priority<900 order by priority desc limit 10";
        logger.debug("try to query candidate evaluation.[SQL]"+sql);
        List<List<Column>> result = jdbcClient.select(sql,queryParams);
        if (result != null && result.size() != 0) {//如果有则直接发射
            for (List<Column> row : result) {
                Values values = new Values();
                for(Column column : row) {
                    values.add(column.getVal());
                }
                this.collector.emit(values);
            }
        }else {//如果没有待处理记录
        		//将所有记录状态更新为pending，并且版本+1
        		sql = "update evaluation set status='pending',revision=revision+1 where priority<900";
        		logger.debug("try to update evaluation revision.[SQL]"+sql);
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
        declarer.declare(new Fields("itemKey","evaluation","type","script","category","featured","itemKey2","evaluation2"));//便于后续查询，作为冗余参数传递
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
