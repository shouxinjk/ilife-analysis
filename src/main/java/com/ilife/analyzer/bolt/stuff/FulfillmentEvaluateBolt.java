
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
 * 根据自定义Groovy脚本完成需要满足度计算
 * 输入：itemKey,categoryId
 * 逻辑：
 * 1，从biz库根据categoryId读取需要满足定义，包含need_id,weight,expression,del_flag
 * 2，从分析库根据itemKey读取已归一处理属性值列表
 * 3，将已归一属性值绑定到Groovy Binding
 * 4，根据需要满足定义逐条计算：
 * 4.1 如果del_flag为1则直接设置为null，否则默认设置为 weight
 * 4.2 执行expression，输出结果更新 weight
 * 5，更新文档库，设置索引状态为pending。并且设置keepNull为false
 * 
 */
public class FulfillmentEvaluateBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(FulfillmentEvaluateBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"itemKey","categoryId"};
    String[] outfields = inputFields;//将数据继续传递
    
    public FulfillmentEvaluateBolt(Properties prop,String database,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
    	//计算需要满足度
    	evaluateFulfillment(tuple);

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
    
    /**
     * 查询需要满足定义，并且采用归一化值计算需要满足度
     */
    private void evaluateFulfillment(Tuple tuple) {
	    //1，根据itemKey从property查询键值对：property,value,score,rank，并准备Groovy脚本参数
	    String sqlQuery = "select property,value,score,rank from property where itemKey=?";
	    logger.debug("try to query item key-values.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("itemKey",tuple.getValueByField("itemKey"),Types.VARCHAR));//itemKey
	    Binding binding = new Binding();
	    List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() > 0) {
	        for (List<Column> row : result) {//逐行解析并放入参数，每一个key包含三个值，格式为property.value-value,property.score-score,property.rank-rank
	        		binding.setVariable(row.get(0).getVal().toString(), row.get(1).getVal());//可以通过键名得到数值，相当于property.value
	        		binding.setVariable(row.get(0).getVal()+".value", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".score", row.get(2).getVal());
	        		binding.setVariable(row.get(0).getVal()+".rank", row.get(3).getVal());
	        }
	    }else {//如果没有对应的值：do nothing
    		logger.debug("Cannot find item values");
	    }
	    //准备Groovy Shell：支持不需要参数的脚本计算
	    GroovyShell shell = new GroovyShell(binding);//准备GroovyShell
	    
	    //2，读取类目上定义的需要满足列表，并计算满足度。 查询条件：categoryId
    	//准备满足度map
		Map<String,Object> fulfillment = Maps.newHashMap();
	    sqlQuery = "select need_id as need,weight,expression as script,del_flag as isDelete from int_category_need where category_id=?";
	    logger.debug("try to query weighted sum.[SQL]"+sqlQuery+"[categoryId]"+tuple.getValueByField("categoryId"));
	    queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("categoryId",tuple.getValueByField("categoryId"),Types.VARCHAR));//categoryId
	    result = jdbcClientBiz.select(sqlQuery,queryParams);
	    for (List<Column> row:result) {
			String needId = row.get(0).getVal().toString();
			double weight = Double.parseDouble(row.get(1).getVal().toString());
			String script = row.get(2).getVal().toString();
			boolean isDelete = Integer.parseInt(row.get(3).getVal().toString())==1;
			if(isDelete) {//如果是删除状态则设置为null，写入时将自动消除
				fulfillment.put(needId, null);
			}else {
				//Groovy脚本计算。脚本可以引用property
				try {
			        Object value = shell.evaluate(script);//返回：text
			        weight = Double.parseDouble(value.toString());
				}catch(Exception ex) {//如果出错了仅做提示，不处理
					logger.error("failed to calculate weight from script.",ex);
				}
				fulfillment.put(needId, weight);
			}
	    }
	    
	    //装配数据
		BaseDocument doc = new BaseDocument();
		doc.setKey(tuple.getStringByField("itemKey"));
		doc.getProperties().put("fulfillment", fulfillment);//fullfillment
		Map<String,Object> status = Maps.newHashMap();
		status.put("satisify", "ready");//更新当前任务状态
		status.put("index", "pending");//通知index任务
		doc.getProperties().put("status", status);//设置状态
		Map<String,Object> timestamp = Maps.newHashMap();
		timestamp.put("satisify", new Date());
//		timestamp.put("index", new Date());
		doc.getProperties().put("timestamp", timestamp);//设置状态
		DocumentUpdateOptions options = new DocumentUpdateOptions();
		options.keepNull(false);//设置keepNull为false

		//同步到arangodb
//		arangoClient.update("my_stuff", doc.getKey(), doc, options); 
		arangoClient.update("my_stuff", doc.getKey(), doc);    	
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
