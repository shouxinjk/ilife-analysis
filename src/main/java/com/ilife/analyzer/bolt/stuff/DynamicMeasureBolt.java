
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
import org.apache.storm.arangodb.common.mapper.ArangoMapper;
import org.apache.storm.arangodb.common.mapper.ArangoUpdateMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoUpdateMapper;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.BaseDocument;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 将顶级维度指标写入arangodb。
 * 预留支持：根据脚本计算客观评价score
 * 输入：itemKey,measure,featured
 * 逻辑：当前直接判断是否是featured字段，是则更新到arangodb
 * 
 */
public class DynamicMeasureBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(DynamicMeasureBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "sea";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"itemKey","dimension","featured","score"};//输入字段包含itemkey和measure，其中featured用于判定是否需要同步到ArangoDB
    String[] outfields = inputFields;//将数据继续传递
    
    public DynamicMeasureBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
    		super(prop,database);
    		this.connectionProviderBiz = connectionProviderBiz;
    		this.connectionProviderAnalyze = connectionProviderAnalyze;
    		this.collection = collection;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	super.prepare(map, topologyContext, collector);
        this.collector = collector;
        connectionProviderBiz.prepare();
        connectionProviderAnalyze.prepare();
        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }

        this.jdbcClientBiz = new JdbcClient(connectionProviderBiz, queryTimeoutSecs);
        this.jdbcClientAnalyze = new JdbcClient(connectionProviderAnalyze, queryTimeoutSecs);
        
    }

    /**
     * 更新到arangodb
     * 
     */
    public void execute(Tuple tuple) {
    	//获取featured信息并同步到arangodb
	 	boolean isFeatured = tuple.getBooleanByField("featured");
	 	if(isFeatured) {//同步到arangodb
	 		syncScore(tuple.getStringByField("itemKey"),
	 				tuple.getStringByField("dimension"),
	 				tuple.getDoubleByField("itemKey"));
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
    
    private void syncScore(String itemKey,String type, double score) {
    	//执行数据更新
		BaseDocument doc = new BaseDocument();
		doc.setKey(itemKey);

		//客观评价维度数值
		Map<String,Object> measure = new HashMap<String,Object>();
		measure.put(type, score);
		doc.getProperties().put("measure", measure);
		doc.getProperties().put(type, score);//直接写入属性
		
		//状态更新
		Map<String,Object> status = new HashMap<String,Object>();
		status.put("measure", "ready");
		status.put("index", "pending");//更改状态再次索引
		doc.getProperties().put("status", status);
		
		//时间戳更新
		Map<String,Object> timestamp = new HashMap<String,Object>();
		timestamp.put("measure", new Date());
		doc.getProperties().put("timestamp", timestamp);
		
		arangoClient.update(collection, doc.getKey(), doc);    	
    }

    @Override
    public void cleanup() {
//    		connectionProviderBiz.cleanup();
//    		connectionProviderAnalyze.cleanup();
    } 
    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
