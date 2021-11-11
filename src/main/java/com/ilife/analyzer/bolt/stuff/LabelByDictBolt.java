
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
import com.arangodb.model.DocumentUpdateOptions;
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
 * 从字典表查询标注值。
 * 
 * 由于preparedStatement不能传递动态表名，此处预先装配表名执行查询
 * 
 */
public class LabelByDictBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(LabelByDictBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"categoryId","propertyId","value","dict","score"};
    String[] outfields = inputFields;//将数据继续传递：仅修改score值为查询得到的字典值
    
    public LabelByDictBolt(Properties prop,String database,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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

        this.jdbcClientBiz = new JdbcClient(connectionProviderBiz, queryTimeoutSecs);
        this.jdbcClientAnalyze = new JdbcClient(connectionProviderAnalyze, queryTimeoutSecs);
        
    }

    public void execute(Tuple tuple) {
    	//查询得到字典标注值
    	double score = findScoreByLabel(tuple);

        //将itemKey、category向后传递
		try {
    		Values values = new Values();
    		for(String field:inputFields) {
    			if("score".equalsIgnoreCase(field)  && score>=0 )//直接替换输入流的score字段：未查到的情况下不影响score值
    				values.add(score);
    			else
    				values.add(tuple.getValueByField(field));
    		}
    		this.collector.emit(values);
	    } catch (Exception e) {
	        this.collector.reportError(e);
	    }
	    Thread.yield();
    }
    
    /**
     * 动态组织字典表查询
     */
    private double findScoreByLabel(Tuple tuple) {
    	String sqlQuery = "select ifnull(score,?) as score from _dict_table where label=?";
    	sqlQuery = sqlQuery.replace("_dict_table", tuple.getStringByField("dict"));
	    logger.debug("try to query by dict.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("score",tuple.getValueByField("score"),Types.DOUBLE));//默认值
	    queryParams.add(new Column("label",tuple.getValueByField("value"),Types.VARCHAR));//标签
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    if (result != null && result.size() > 0) {
            for (List<Column> row : result) {
        		for(Column column:row) {//仅返回一个score字段，此处确认
        			if("score".equalsIgnoreCase(column.getColumnName())) {
        				return Double.parseDouble(column.getVal().toString());
        			}
        		}
        }	        
	    }else {//如果没有对应的值：do nothing
    		logger.debug("Cannot find label from dict");
	    }
	    return -1;
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
