
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
 * 根据自定义Groovy脚本完成主观评价维度计算
 * 输入：itemKey,evaluate,type,method(method类型有三种：weighted-sum,script,system，其中script为groovy脚本本身,system指内置算法),
 * 逻辑：根据method及type进行补充的处理计算
 * 
 */
public class DynamicEvaluateBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(DynamicEvaluateBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "sea";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"itemKey","evaluation","type","script","category","featured","itemKey2","evaluation2"};//输入字段包含itemkey和category，其中isFeature用于判定是否需要同步到ArangoDB
    String[] outfields = inputFields;//将数据继续传递
    
    public DynamicEvaluateBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
     * 根据类型分别执行不同的算法。
     * perform：默认执行weigted-sum，查询关联evaluate-measure并加权汇总，更新score，包含abcde五个字段
     * cost：默认执行weigted-sum，查询关联evaluate-measure并加权汇总，更新score，包含xyz三个字段
     * constraint：默认执行script，查询item后执行groovy脚本，更新text
     * satisify：默认执行system算法，查询对应category关联需求满足，转换为tag列表后更新text
     * context：默认执行system算法，查询对应category关联的诱因，转换为tag列表后更新text
     * style：默认执行script，查询item后执行groovy脚本，更新text
     * tag：默认执行script，从用户行为等获取标签列表
     */
    public void execute(Tuple tuple) {
    	 	String type = tuple.getStringByField("type");//获取type字段
    	 	//根据type分别处理
    	 	if("a".equalsIgnoreCase(type)||"b".equalsIgnoreCase(type)||"c".equalsIgnoreCase(type)||"d".equalsIgnoreCase(type)||"e".equalsIgnoreCase(type)) {//perform 包含的5个字段
    	 		evaluateWeightedSumByScore(tuple);
    	 	}else if("x".equalsIgnoreCase(type)||"y".equalsIgnoreCase(type)||"z".equalsIgnoreCase(type)) {//cost 包含3个字段
    	 		evaluateWeightedSumByScore(tuple);
    	 	}else if("satisify".equalsIgnoreCase(type)) {
    	 		evaluateSatisfyByText(tuple);
    	 	}else if("context".equalsIgnoreCase(type)) {
    	 		evaluateContextByText(tuple);
    	 	}else if("ignore".equalsIgnoreCase(type)||"do-not-care".equalsIgnoreCase(type)) {//ignore：表示不需要处理的字段
    	 		logger.debug("Ignore type.[type]"+type);
    	 	}else {//否则均通过脚本进行处理，包括style、constraint、tags等
    	 		evaluateScriptByText(tuple);
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


    /**
     * 汇总需求满足度字符串     
     */
    private void evaluateSatisfyByText(Tuple tuple) {
	    	//1，根据category查询所有关联的motivation
	    String sqlQuery = "select name from mod_motivation where id in (select motivation_ids from mod_item_category where id=?)";
	    logger.debug("try to query related motivations.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("category",tuple.getValueByField("category"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    StringBuffer sb = new StringBuffer();
	    if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
                for(Column column : row) {
                    sb.append(column.getVal().toString());
                    sb.append(" ");
                }
            }
	        //2，更新到数据库
	        //update evaluate set text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY';
	    		String sqlUpdate = "update evaluation set text='_TEXT',status='ready' where evaluation='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY'"
			    		.replace("_TEXT", sb.toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_ITEMKEY", tuple.getValueByField("itemKey").toString());
	    		logger.debug("try to update constraint.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	       
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("itemKey").toString(),tuple.getValueByField("type").toString(),sb.toString());
	        }
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate satisfication.");
	    }
    }   

    /**
     * 汇总需求满足度字符串     
     */
    private void evaluateContextByText(Tuple tuple) {
	    	//1，根据category查询所有关联的motivation
	    String sqlQuery = "select name from mod_occasion where id in (select occasion_ids from mod_item_category where id=?)";
	    logger.debug("try to query related occasions.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("category",tuple.getValueByField("category"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    StringBuffer sb = new StringBuffer();
	    if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
                for(Column column : row) {
                    sb.append(column.getVal().toString());
                    sb.append(" ");
                }
            }
	        //3，更新到数据库
	        //update evaluate set text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY';
	    		String sqlUpdate = "update evaluation set text='_TEXT',status='ready' where evaluation='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY'"
			    		.replace("_TEXT", sb.toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_ITEMKEY", tuple.getValueByField("itemKey").toString());
	    		logger.debug("try to update constraint.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("itemKey").toString(),tuple.getValueByField("type").toString(),sb.toString());
	        }	        
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate satisfication.");
	    }
    }   
    /**
     * 加权汇总计算得分
     * 适用于perform、cost      
     */
    private void evaluateWeightedSumByScore(Tuple tuple) {
	    	//1，根据itemKey查询键值对并进行加权汇总：查询条件：evaluate,type,itemKey
	    String sqlQuery = "select sum(em.weight*m.score) as score from evaluation e,evaluation_measure em,measure m where e.itemKey=? and e.evaluation=? and e.type=? and e.evaluation=em.evaluation and em.type=? and em.dimension=m.dimension";
	    logger.debug("try to query weighted sum.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("itemKey",tuple.getValueByField("itemKey"),Types.VARCHAR));//itemKey
	    queryParams.add(new Column("evaluation",tuple.getValueByField("evaluation"),Types.VARCHAR));//evaluation
	    queryParams.add(new Column("type",tuple.getValueByField("type"),Types.VARCHAR));//type：过滤evaluate
	    queryParams.add(new Column("type2",tuple.getValueByField("type"),Types.VARCHAR));//type：过滤evaluate-measure
	    List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	    		List<Column> row = result.get(0);
	        
	        //2，更新到数据库
	        //update evaluate set score='_SCORE',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY';
	    		String sqlUpdate = "update evaluation set score='_SCORE',status='ready' where evaluation='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY'"
			    		.replace("_SCORE", row.get(0).getVal().toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_ITEMKEY", tuple.getValueByField("itemKey").toString());
	    		logger.debug("try to update dynamic calculate result.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        //3，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncScore(tuple.getValueByField("itemKey").toString(),tuple.getValueByField("type").toString(),Double.parseDouble(row.get(1).getVal().toString()));
	        }	        
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to weighted-sum evaluation-measure score");
	    }
    }   
    
    /**
     * 根据自定以脚本进行计算，并更新text字段
     * 适用于constraint、style
     */
    private void evaluateScriptByText(Tuple tuple) {
	    	//1，根据itemKey从property查询键值对：property,value,score,rank，并准备脚本参数
	    String sqlQuery = "select property,value,score,rank from property where itemKey=?";
	    logger.debug("try to query item key-values.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("itemKey",tuple.getValueByField("itemKey"),Types.VARCHAR));//itemKey
	    Binding binding = new Binding();
	    List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	        for (List<Column> row : result) {//逐行解析并放入参数，每一个key包含三个值，格式为property.value-value,property.score-score,property.rank-rank
	        		binding.setVariable(row.get(0).getVal().toString(), row.get(1).getVal());//可以通过键名得到数值，相当于property.value
	        		binding.setVariable(row.get(0).getVal()+".value", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".score", row.get(2).getVal());
	        		binding.setVariable(row.get(0).getVal()+".rank", row.get(3).getVal());
	        }

	        //2，Groovy脚本计算。脚本中引用变量需要通过property+value/score/rank的组合
	        GroovyShell shell = new GroovyShell(binding);
	        Object value = shell.evaluate(tuple.getValueByField("script").toString());//返回：text

	        //3，更新到数据库
	        //update evaluate set score='_SCORE',text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY';
	    		String sqlUpdate = "update evaluation set text='_TEXT',status='ready' where evaluation='_EVALUATE' and type='_TYPE' and itemKey='_ITEMKEY'"
			    		.replace("_TEXT", value.toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_ITEMKEY", tuple.getValueByField("itemKey").toString());
	    		logger.debug("try to update dynamic calculate result.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("itemKey").toString(),tuple.getValueByField("type").toString(),value.toString());
	        }
	    }else {//如果没有对应的值：do nothing
	    		logger.debug("Cannot find item values");
	    }
    }
    
    private void syncScore(String itemKey,String type, double score) {
    	//执行数据更新
		BaseDocument doc = new BaseDocument();
		doc.setKey(itemKey);

		//主观评价维度更新
		Map<String,Object> evaluate = new HashMap<String,Object>();
		evaluate.put(type, score);
		if("abcde".indexOf(type)>=0)
			doc.getProperties().put("performance", evaluate);
		else
			doc.getProperties().put("cost", evaluate);
		
		//状态更新
		Map<String,Object> status = new HashMap<String,Object>();
		status.put("evaluate", "ready");
		status.put("index", "pending");//更改状态再次索引
		doc.getProperties().put("status", status);
		
		//时间戳更新
		Map<String,Object> timestamp = new HashMap<String,Object>();
		timestamp.put("evaluate", new Date());
		doc.getProperties().put("timestamp", timestamp);

		arangoClient.update(collection, doc.getKey(), doc);    	
    }
    
    private void syncText(String itemKey,String type, String text) {
		//执行数据更新
		BaseDocument doc = new BaseDocument();
		doc.setKey(itemKey);
	
		//主观评价维度更新
		Map<String,Object> evaluate = new HashMap<String,Object>();
		evaluate.put(type, text);
		doc.getProperties().put("style", evaluate);//指定到偏好设置
		
		//状态更新
		Map<String,Object> status = new HashMap<String,Object>();
		status.put("evaluate", "ready");
		status.put("index", "pending");//更改状态再次索引
		doc.getProperties().put("status", status);
		
		//时间戳更新
		Map<String,Object> timestamp = new HashMap<String,Object>();
		timestamp.put("evaluate", new Date());
		doc.getProperties().put("timestamp", timestamp);

		arangoClient.update(collection, doc.getKey(), doc);    	
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
