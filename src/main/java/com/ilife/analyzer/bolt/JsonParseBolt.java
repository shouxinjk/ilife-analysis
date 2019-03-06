
package com.ilife.analyzer.bolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.arangodb.bolt.AbstractArangoBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.ilife.analyzer.util.JsonParseUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * 解析JSON字符串为键值对
 */
public class JsonParseBolt extends BaseRichBolt {
	
    private static final Logger logger = LoggerFactory.getLogger(JsonParseBolt.class);
    protected OutputCollector collector;
    transient Gson gson = new Gson();
    String[] inputFields = {"_doc"};//需要输入的字段，第一个必须是json字段
    String[] outfields = {"property","value"};
    
    public JsonParseBolt(String... fields) {
    		this.inputFields = fields;
    }
    
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    		this.collector = collector;
    }

    public void execute(Tuple tuple) {
    		String json = tuple.getValueByField(inputFields[0]).toString();
    		logger.debug("try to parse json to row-column values.[json]"+json);
    		Map<String,Object> map = new HashMap<String,Object>();
        JsonParseUtil.parseJson2Map(map,json,null);
        Iterator<Entry<String,Object>> iter= map.entrySet().iterator();
        while(iter.hasNext()) {
        		Entry<String,Object> entry = iter.next();
	        try {
		    		Values values = new Values();
		    		//添加key、value
		    		values.add(entry.getKey());
		    		values.add(entry.getValue().toString());
		    		//添加固定字段，如category等
		    		for(int i=1;i<inputFields.length;i++) {//获取其他字段
		    			values.add(tuple.getValueByField(inputFields[i]).toString());
		    		}
		    		this.collector.emit(values);
		    } catch (Exception e) {
		        this.collector.reportError(e);
		    }
        }
	    Thread.yield();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
