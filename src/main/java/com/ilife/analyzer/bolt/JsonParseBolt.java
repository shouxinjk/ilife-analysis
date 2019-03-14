
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
    String[] outfields = {"property","value","category","itemKey"};
    
//    public JsonParseBolt(String... fields) {
//    		this.inputFields = fields;
//    }
    
    public JsonParseBolt(String[] input,String[] output) {
		this.inputFields = input;
		this.outfields = output;
    }
    
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    		this.collector = collector;
    }

    public void execute(Tuple tuple) {
    		Map<String,Object> map = (Map<String,Object>)tuple.getValueByField("_doc");
    		parse("",map,tuple);
	    Thread.yield();
    }
    
    /**
     * 解析Map数据，即键值对。
     * @param prefix：键值前缀
     * @param map：待解析Map数据
     * @param tuple：当前tuple，用于获取category、_key等固定字段
     */
    private void parse(String prefix,Map<String,Object> map,Tuple tuple) {
		logger.debug("===map=== [prefix]"+prefix+"[map]"+map);
	    Iterator<Entry<String,Object>> iter= map.entrySet().iterator();
	    while(iter.hasNext()) {
	    		Entry<String,Object> entry = iter.next();
	    		String key = prefix.trim().length()==0?entry.getKey():prefix+"."+entry.getKey();
	    		if(entry.getValue() instanceof Map<?,?>) {//embed key:s
	    			Map<String,Object> map2 = (Map<String,Object>)entry.getValue();
	    			parse(key,map2,tuple);
	    		}else if(entry.getValue() instanceof List<?>) {//embed key:s array
	    			List<Object> list = (List<Object>)entry.getValue();
	    			//判定类型
	    			if(list.get(0)!=null && list.get(0) instanceof Map<?,?>) {//Map列表则逐个解析
	    				logger.debug("===map array item=== [prefix]"+prefix+"[map]"+entry.getValue());
		    			int i=0;
		    			for(Object obj:list) {
		    				Map<String,Object> m = (Map<String,Object>)obj;
		    				parse(key+"."+(i++),m,tuple);//map数组内通过添加数字序列后缀，假设拥有相似的字段
		    				//parse(key,m,tuple);//注意：假设数组内的key值不同，如果数组内有相同key值，记录会被覆盖。
		    			}
	    			}else {
	    				logger.debug("===array item=== [prefix]"+prefix+"[value]"+entry.getValue());
	    				parse(key,entry.getValue(),tuple);
	    			}
	    		}else {
	    			parse(key,entry.getValue(),tuple);
	    		}
	    }    	
    }
    
    /**
     * 解析单个数值，直接发送key:value对
     * @param key：数据键
     * @param value：数据值
     * @param tuple：当前tuple，用于获取category、_key等固定字段
     */
    private void parse(String key, Object value, Tuple tuple) {
		logger.debug("===value=== [key]"+key+"[value]"+value);
        try {
	    		Values values = new Values();
	    		//添加key、value
	    		values.add(key);
	    		values.add(""+value);
	    		//添加固定字段，如category等
	    		for(int i=1;i<inputFields.length;i++) {//获取其他字段
	    			values.add(tuple.getValueByField(inputFields[i]));
	    		}
	    		this.collector.emit(values);
	    } catch (Exception e) {
	        this.collector.reportError(e);
	    } 	
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
