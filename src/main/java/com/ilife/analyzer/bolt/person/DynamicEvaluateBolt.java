
package com.ilife.analyzer.bolt.person;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 根据自定义Groovy脚本完成主观评价维度计算
 * 输入：userKey,evaluate,type,method(method类型有三种：weighted-sum,script,system，其中script为groovy脚本本身,system指内置算法),
 * 逻辑：根据method及type进行补充的处理计算
 * 
 */
public class DynamicEvaluateBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(DynamicEvaluateBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "forge";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"userKey","evaluation","type","script","persona","featured","userKey2","evaluation2"};//输入字段包含itemkey和persona，其中isFeature用于判定是否需要同步到ArangoDB
    String[] outfields = inputFields;//将数据继续传递
    
    public DynamicEvaluateBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
    		super(prop,database);
    		this.connectionProviderBiz = connectionProviderBiz;
    		this.connectionProviderAnalyze = connectionProviderAnalyze;
    		this.collection = collection;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
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
     * perform：默认执行weigted-sum，查询关联evaluate-measure并加权汇总，更新score
     * cost：默认执行weigted-sum，查询关联evaluate-measure并加权汇总，更新score
     * constraint：默认执行script，查询item后执行groovy脚本，更新text
     * satisify：默认执行system算法，查询对应persona关联需求满足，转换为tag列表后更新text
     * context：默认执行system算法，查询对应persona关联的诱因，转换为tag列表后更新text
     * style：默认执行script，查询item后执行groovy脚本，更新text
     * persona：默认执行system算法，选择得分匹配最高的更新text
     * hierarchy：不关注。通过EvaluatePersona Topology 完成
     * phase：不关注。通过EvaluatePersona Topology 完成
     */
    public void execute(Tuple tuple) {
    	 	String type = tuple.getStringByField("type");//获取type字段
    	 	//根据type分别处理
    	 	if("perform".equalsIgnoreCase(type)) {
    	 		evaluateWeightedSumByScore(tuple);
    	 	}else if("cost".equalsIgnoreCase(type)) {
    	 		evaluateWeightedSumByScore(tuple);
    	 	}else if("constraint".equalsIgnoreCase(type)) {
    	 		evaluateScriptByText(tuple);
    	 	}else if("satisify".equalsIgnoreCase(type)) {
    	 		evaluateSatisfyByText(tuple);
    	 	}else if("context".equalsIgnoreCase(type)) {
    	 		evaluateContextByText(tuple);
    	 	}else if("style".equalsIgnoreCase(type)) {
    	 		evaluateScriptByText(tuple);
    	 	}else if("persona".equalsIgnoreCase(type)) {
    	 		evaluatePersonaByText(tuple);
    	 	}else {//hierarchy、phase通过EvaluatePersona拓扑完成，此处不做处理
    	 		logger.debug("Ingnore type.[type]"+type);
    	 	}

        //将userKey、persona向后传递
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
     * 获取得分最高的persona并更新     
     */
    private void evaluatePersonaByText(Tuple tuple) {
	    	//1，查询得分最高的user-persona。注意仅仅限制得分最高的一条。
	    String sqlQuery = "select personaId from user_persona where userKey=? order by scoreHierarchy desc,scorePhase desc, scorePersona desc limit 1";
	    logger.debug("try to query top listed user-persona.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("persona",tuple.getValueByField("userKey"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    String personaId = "";
	    if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
                for(Column column : row) {
                    personaId = column.getVal().toString();//取得personaId
                }
            }
	        //2，更新到数据库：更新指定评价结果
	        //update evaluate set text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and userKey='_USERKEY';
	    		String sqlUpdate = "update user_evaluation set text='_TEXT',status='ready' where user_evaluation='_EVALUATE' and type='_TYPE' and userKey='_USERKEY'"
			    		.replace("_TEXT", personaId)
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    		logger.debug("try to update persona.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        
	        //3，更新到数据库：在所有评价结果中更新用户对应persona
	    		String sqlUpdate2 = "update user_evaluation set persona='_TEXT',modifiedOn=now() where and userKey='_USERKEY'"
			    		.replace("_TEXT", personaId)
			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    		logger.debug("try to update persona for all records.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 	       
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("_key").toString(),tuple.getValueByField("type").toString(),personaId);
	        }
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate satisfication.");
	    }
    }   

    /**
     * 汇总需求满足度字符串     
     */
    private void evaluateSatisfyByText(Tuple tuple) {
	    	//1，根据persona查询所有关联的motivation
	    String sqlQuery = "select name from mod_motivation where id in (select motivation_ids from mod_life_style where persona_id=? limit 1)";
	    logger.debug("try to query related motivations.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("persona",tuple.getValueByField("persona"),Types.VARCHAR));
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
	        //update evaluate set text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and userKey='_USERKEY';
	    		String sqlUpdate = "update user_evaluation set text='_TEXT',status='ready' where user_evaluation='_EVALUATE' and type='_TYPE' and userKey='_USERKEY'"
			    		.replace("_TEXT", sb.toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    		logger.debug("try to update constraint.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	       
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("_key").toString(),tuple.getValueByField("type").toString(),sb.toString());
	        }
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate satisfication.");
	    }
    }   

    /**
     * 汇总需求满足度字符串     
     */
    private void evaluateContextByText(Tuple tuple) {
	    	//1，根据persona查询所有关联的motivation
	    String sqlQuery = "select name from mod_occasion where id in (select occasion_ids from mod_life_style where persona_id=? limit 1)";
	    logger.debug("try to query related occasions.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("persona",tuple.getValueByField("persona"),Types.VARCHAR));
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
	        //update evaluate set text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and userKey='_USERKEY';
	    		String sqlUpdate = "update user_evaluation set text='_TEXT',status='ready' where user_evaluation='_EVALUATE' and type='_TYPE' and userKey='_USERKEY'"
			    		.replace("_TEXT", sb.toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    		logger.debug("try to update constraint.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("_key").toString(),tuple.getValueByField("type").toString(),sb.toString());
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
	    	//1，根据userKey查询键值对并进行加权汇总：查询条件：evaluate,type,userKey
	    String sqlQuery = "select sum(em.weight*m.score) as score from user_evaluation e,user_evaluation_measure em,user_measure m where e.userKey=? and e.evaluation=? and e.type=? and e.evaluation=em.evaluation and em.type=? and em.dimension=m.dimension";
	    logger.debug("try to query weighted sum.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("userKey",tuple.getValueByField("userKey"),Types.VARCHAR));//userKey
	    queryParams.add(new Column("evaluation",tuple.getValueByField("evaluation"),Types.VARCHAR));//evaluation
	    queryParams.add(new Column("type",tuple.getValueByField("type"),Types.VARCHAR));//type：过滤evaluate
	    queryParams.add(new Column("type2",tuple.getValueByField("type"),Types.VARCHAR));//type：过滤evaluate-measure
	    List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	    		List<Column> row = result.get(0);
	        
	        //2，更新到数据库
	        //update evaluate set score='_SCORE',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and userKey='_USERKEY';
	    		String sqlUpdate = "update user_evaluation set score='_SCORE',status='ready' where evaluation='_EVALUATE' and type='_TYPE' and userKey='_USERKEY'"
			    		.replace("_SCORE", row.get(0).getVal().toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    		logger.debug("try to update dynamic calculate result.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        //3，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncScore(tuple.getValueByField("_key").toString(),tuple.getValueByField("type").toString(),Double.parseDouble(row.get(1).getVal().toString()));
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
	    	//1，根据userKey从property查询键值对：property,value,score,rank，并准备脚本参数
	    String sqlQuery = "select property,value,score,rank from user_property where userKey=?";
	    logger.debug("try to query item key-values.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("userKey",tuple.getValueByField("_key"),Types.VARCHAR));//userKey
	    Binding binding = new Binding();
	    List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	        for (List<Column> row : result) {//逐行解析并放入参数，每一个key包含三个值，格式为property.value-value,property.score-score,property.rank-rank
	        		binding.setVariable(row.get(0).getVal().toString(), row.get(0).getVal());//可以通过键名得到数值，相当于property.value
	        		binding.setVariable(row.get(0).getVal()+".value", row.get(0).getVal());
	        		binding.setVariable(row.get(0).getVal()+".score", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".rank", row.get(2).getVal());
	        }

	        //2，Groovy脚本计算。脚本中引用变量需要通过property+value/score/rank的组合
	        GroovyShell shell = new GroovyShell(binding);
	        Object value = shell.evaluate(tuple.getValueByField("script").toString());//返回：text

	        //3，更新到数据库
	        //update evaluate set score='_SCORE',text='_TEXT',status='ready' where evaluate='_EVALUATE' and type='_TYPE' and userKey='_USERKEY';
	    		String sqlUpdate = "update user_evaluation set text='_TEXT',status='ready' where evaluation='_EVALUATE' and type='_TYPE' and userKey='_USERKEY'"
			    		.replace("_TEXT", value.toString())
			    		.replace("_EVALUATE", tuple.getValueByField("evaluation").toString())
			    		.replace("_TYPE", tuple.getValueByField("type").toString())
			    		.replace("_USERKEY", tuple.getValueByField("_key").toString());
	    		logger.debug("try to update dynamic calculate result.[SQL]"+sqlUpdate);
	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        
	        //4，同步到Arango
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("_key").toString(),tuple.getValueByField("type").toString(),value.toString());
	        }
	    }else {//如果没有对应的值：do nothing
	    		logger.debug("Cannot find item values");
	    }
    }


    private void syncScore(String userKey,String type, double score) {
    		//执行数据更新
		BaseDocument doc = new BaseDocument();
		doc.setKey(userKey);

		Map<String,Object> evaluate = new HashMap<String,Object>();
		evaluate.put(type, score);
		doc.getProperties().put("evaluate", evaluate);
		doc.getProperties().put("status", "pending");//更改状态再次索引
		arangoClient.update(collection, doc.getKey(), doc);    	
    }
    
    private void syncText(String userKey,String type, String text) {
		//执行数据更新
		BaseDocument doc = new BaseDocument();
		doc.setKey(userKey);
	
		Map<String,Object> evaluate = new HashMap<String,Object>();
		evaluate.put(type, text);
		doc.getProperties().put("evaluate", evaluate);
		doc.getProperties().put("status", "pending");//更改状态再次索引
		
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
