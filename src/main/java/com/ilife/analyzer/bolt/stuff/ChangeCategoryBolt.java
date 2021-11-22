
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
import org.apache.storm.shade.com.google.common.collect.Maps;
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
 * 将匹配的category写入 my_stuff
 * 更新meta.category字段，同时更新状态及时间戳
 * 
 * 批量更新逻辑：
 * for doc in my_stuff filter doc.category==@category and doc.source==@platform 
 * update doc with {meta:{category:@mappingId},status:{classify:'done'},timestamp:{classify:'now()'}} 
 * in my_stuff return NEW
 * 
 */
public class ChangeCategoryBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(ChangeCategoryBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "sea";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"mappingId","category","platform"};
    String[] outfields = inputFields;//将数据继续传递
    
    public ChangeCategoryBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
     * 直接更新meta.category
     */
    public void execute(Tuple tuple) {
    	//批量更新meta.category
    	syncDoc(tuple);
    	
        //将数据向后传递
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

    private void syncDoc(Tuple tuple) {
		Map<String,Object> params = Maps.newHashMap();
		params.put("category", tuple.getStringByField("category"));
		params.put("platform", tuple.getStringByField("platform"));
		params.put("mappingId", tuple.getStringByField("mappingId"));
		params.put("timestamp", new Date());
		
		arangoClient.query("for doc in my_stuff filter doc.category==@category and doc.source==@platform "
				+ " update doc with {meta:{category:@mappingId},status:{classify:'ready'},timestamp:{classify:@timestamp}} "
				+ " in my_stuff return NEW", params, BaseDocument.class);    	
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
