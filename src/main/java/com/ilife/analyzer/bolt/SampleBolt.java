
package com.ilife.analyzer.bolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
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

import com.arangodb.entity.BaseDocument;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 更新评价计算版本：根据itemKey更新对应的版本项。
 * 
 * FOR doc in my_stuff filter doc._key == "ITEMKEY" update doc with { version: {minor:{perform:doc.version.minor.perform+1}} } in my_stuff return NEW
 * FOR doc in my_stuff filter doc._key == "71dcd0bd30bec20710187c374e9cd202" update doc with { version: {major:doc.version.major+1},modifiedOn:DATE_NOW() } in my_stuff return NEW
 * 
 */
public class SampleBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(SampleBolt.class);
    transient Gson gson = new Gson();
    String[] fields = {};
    public SampleBolt(Properties prop,String database,String... fields) {
        super(prop,database);
        this.fields = fields;
    }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
    }

    public void execute(Tuple tuple) {
        try {
	    		logger.info("[JSON]"+tuple.getValueByField("_doc"));
	    		//执行数据更新
	    		BaseDocument doc = new BaseDocument();
        		doc.setKey(tuple.getStringByField("_key"));

        		Map<String,Object> version = new HashMap<String,Object>();
        		version.put("major", "骚货");
        		
        		Map<String,Object> minor = new HashMap<String,Object>();
        		minor.put("咪咪", "坚挺酥胸");
        		minor.put("木耳", "粉嫩");
        		minor.put("舌头", "湿淋淋");
        		minor.put("屁股", "圆润肥大");
        		version.put("minor", minor);
        		doc.getProperties().put("version", version);
        		arangoClient.update("my_stuff_test", doc.getKey(), doc);
        		
	    		//继续向后传递
	    		Values values = new Values();
	    		values.add(tuple.getValueByField("_key"));
	    		values.add(tuple.contains("test")?"原值:"+tuple.getValueByField("test"):"咪咪超级大啊");
	    		values.add("原值:"+tuple.getValueByField("source"));
	    		this.collector.emit(values);
	    } catch (Exception e) {
	        this.collector.reportError(e);
	    }
	    Thread.yield();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(fields));
    }
}
