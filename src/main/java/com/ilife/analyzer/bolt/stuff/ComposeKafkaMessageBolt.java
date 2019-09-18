
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
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.ilife.analyzer.serializer.Stuff;
import com.ilife.analyzer.util.BeanUtil;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

/**
 * 将ArangoDB doc转换为Java对象，并序列化发送Kafka消息
 * 
 */
public class ComposeKafkaMessageBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(ComposeKafkaMessageBolt.class);
    protected OutputCollector collector;

    String[] outfields = {"message"};//默认输出字段为message，用于发送到kafka。如果有其他字段需要从输入字段中获取
    
    public ComposeKafkaMessageBolt() {
    }
    public ComposeKafkaMessageBolt(String[] outputFields) {
		this.outfields = outputFields;
}
    public void execute(Tuple tuple) {
    		//Stuff stuff = new Stuff();
    		HashMap map = (HashMap)tuple.getValueByField("_doc");
    		map.put("_key", tuple.getValueByField("_key"));//将_key加入作为唯一识别码
    		if(map.get("tagging")==null) {//自动索引情况下添加tagging
    			map.put("tagging", map.get("title"));//TODO 需要使用自动摘要得到
    		}
    		logger.error("try to compose kafka msg."+map);
    		/**
	    	try {
	    		logger.error("try to compose kafka msg."+map);
	    		stuff = (Stuff) BeanUtil.mapToBean(map, Stuff.class);
	    	}catch(Exception ex) {
	    		logger.error("error while convert doc to kafka msg.",ex);
	    	}
	    	//**/
    		try {
	    		Values values = new Values();
	    		for(String field:outfields) {
	    			if("message".equalsIgnoreCase(field)) {
	    				values.add(map);
	    			}else
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

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}
