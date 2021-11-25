
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
import com.ilife.analyzer.util.Util;

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
 * 在装载时，将stuff的属性写入platform_properties，等待建立映射
 * 
 */
public class InsertPropertyBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(InsertPropertyBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String database= "sea";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"source","property","category","categoryId","meta.category"};//输入字段：来源、属性名称、原始类目名称、原始类目ID（可能为空）、已经映射后的标准类目ID
    String[] outfields = inputFields;//将数据继续传递
    
    public InsertPropertyBolt(Properties prop,String database,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
    		super(prop,database);
    		this.connectionProviderBiz = connectionProviderBiz;
    		this.connectionProviderAnalyze = connectionProviderAnalyze;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	super.prepare(map, topologyContext, collector);
        this.collector = collector;
        connectionProviderBiz.prepare();
        connectionProviderAnalyze.prepare();
        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }
//
//        this.jdbcClientBiz = new JdbcClient(connectionProviderBiz, queryTimeoutSecs);
//        this.jdbcClientAnalyze = new JdbcClient(connectionProviderAnalyze, queryTimeoutSecs);
        
    }

    /**
     * 将属性信息写入platform_properties等待标注
     * 
     */
    public void execute(Tuple tuple) {
    	String itemKey = Util.md5(tuple.getStringByField("source")+tuple.getStringByField("property"));
    	logger.debug("try to insert item to platform_categories.",tuple.getValues());
    	//查询是否已经存在
		BaseDocument doc = arangoClient.find("platform_categories", itemKey);
		if(doc == null) {
			doc = new BaseDocument();
			doc.getProperties().put("source", tuple.getStringByField("source"));
			doc.getProperties().put("category", tuple.getStringByField("category"));//原始类目名称
			doc.getProperties().put("cid", tuple.getStringByField("categoryId"));//原始类目ID
			doc.getProperties().put("mappingCategoryId", tuple.getStringByField("meta.category"));//映射的标准类目ID
			doc.getProperties().put("name", tuple.getStringByField("property"));
			doc.setKey(itemKey);
			logger.debug("try to insert item to platform_categories.[properties]",doc.getProperties());
			arangoClient.insert("platform_categories", doc);
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

    @Override
    public void cleanup() {
    	
    } 
    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
