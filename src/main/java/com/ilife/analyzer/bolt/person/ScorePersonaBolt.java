
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
 * 计算用户阶层、阶段、画像得分。通过用户数据，分别计算阶层匹配得分、阶段匹配得分、画像匹配得分。
 * 输入：userKey,hierarchyId,phaseId,personaId,script
 * 逻辑：根据method及type进行补充的处理计算
 * 1，根据UserKey读取用户属性列表，包括客观评价、主观评价
 * 2.1，计算hierarchy score：根据hierarchyId读取配置的资本得分，并加权计算得到参照值sample。与用户评价hierarchy匹配得到差异，作为得分。计算方法：（hierarchy-sample）/max(sample,hierarchy)。更新得分到hierarchScore字段
 * 2.2，计算phase score：根据phaseId读取阶段的配置记录，并将用户信息传入脚本进行计算，如果返回为true，则得分为1，否则为0，更新得分到phaseScore字段。注意：阶段评价的不同记录间需要保持互斥
 * 2.3，调用匹配脚本script，计算得分并更新到score字段
 * 
 */
public class ScorePersonaBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(ScorePersonaBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "forge";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"userKey","hierarchyId","phaseId","personaId"};
    String[] outfields = inputFields;//将数据继续传递
    
    public ScorePersonaBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
        super.prepare(map, topologyContext, collector);
    }

    public void execute(Tuple tuple) {
    	 	//1，根据userKey获取所有属性，包括客观评价和主观评价
    		Binding profile = getUserProfile(tuple.getStringByField("userKey"));
    		
    	 	//2，计算hierarchy score：根据hierarchyId读取识别脚本并计算。更新得分到hierarchScore字段
    		scoreHierarchy(tuple,profile);
    		
    		//3，计算phase score：根据phaseId读取阶段配置脚本并计算，更新得分到phaseScore字段
    		scorePhase(tuple,profile);
    		
    	 	//4，调用匹配脚本script，计算得分并更新到score字段
    		scorePersona(tuple,profile);

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
     * 评价所属阶层
     * 1，读取hierarchyId对应的阶层。包括script    
     * 2，传入profile进行脚本计算
     * 3，根据userKey、hierarchyID更新对应记录的scoreHierarchy得分
     */
    private void scoreHierarchy(Tuple tuple,Binding binding) {
	    	//1，根据hierarchyId读取阶层配置
	    String sqlQuery = "select expression as script from mod_hierarchy where id=? limit 1";
	    logger.debug("try to query candidate hierarchy.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("hierarchyId",tuple.getValueByField("hierarchyId"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    String script = "";
	    if (result != null && result.size() != 0) {
	    		script = result.get(0).get(0).getVal().toString();//获取脚本
	    		
	    		//2，Groovy脚本计算。脚本中引用变量需要通过property+value/score/rank的组合
	        GroovyShell shell = new GroovyShell(binding);
	        Object value = shell.evaluate(script);//返回得分：double
	        try {
	        		double score = Double.parseDouble(""+value);
	    	        
	    	        //3，更新到数据库
	    	    		String sqlUpdate = "update user_persona set scoreHierarchy='_SCORE',modifiedOn=now() where hierarchyId='_HIERARCHYID' and userKey='_USERKEY'"
	    			    		.replace("_SCORE", ""+score)
	    			    		.replace("_HIERARCHYID", tuple.getValueByField("hierarchyId").toString())
	    			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    	    		logger.debug("try to update hierarchy score to user_persona.[SQL]"+sqlUpdate);
	    	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	    	       
	        }catch(Exception ex) {
	        		logger.error("the result cannot be parsed to double value.[result]"+value,ex);
	        }
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate user hierarchy.");
	    }
    }   

    /**
     * 评价所属阶段
     * 1，读取phaseId对应的阶段。包括script    
     * 2，传入profile进行脚本计算
     * 3，根据userKey、phaseId更新对应记录的scorePhase得分
     */
    private void scorePhase(Tuple tuple,Binding binding) {
	    	//1，根据phaseId读取阶段配置
	    String sqlQuery = "select expression as script from mod_phase where id=? limit 1";
	    logger.debug("try to query candidate phase.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("phaseId",tuple.getValueByField("phaseId"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    String script = "";
	    if (result != null && result.size() != 0) {
	    		script = result.get(0).get(0).getVal().toString();//获取脚本
	    		
	    		//2，Groovy脚本计算。脚本中引用变量需要通过property+value/score/rank的组合
	        GroovyShell shell = new GroovyShell(binding);
	        Object value = shell.evaluate(script);//返回得分：double
	        try {
	        		double score = Double.parseDouble(""+value);
	    	        
	    	        //3，更新到数据库
	    	    		String sqlUpdate = "update user_persona set scorePhase='_SCORE',modifiedOn=now() where phaseId='_PHASEID' and userKey='_USERKEY'"
	    			    		.replace("_SCORE", ""+score)
	    			    		.replace("_PHASEID", tuple.getValueByField("phaseId").toString())
	    			    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
	    	    		logger.debug("try to update phase score to user_persona.[SQL]"+sqlUpdate);
	    	        jdbcClientAnalyze.executeSql(sqlUpdate); 
	    	       
	        }catch(Exception ex) {
	        		logger.error("the result cannot be parsed to double value.[result]"+value,ex);
	        }
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate user phase.");
	    }
    }   
    
    /**
     * 评价所属阶段
     * 1，读取personaId对应的分群。包括script    
     * 2，传入profile进行脚本计算
     * 3，根据userKey、personaId更新对应记录的scorePersona得分
     */
    private void scorePersona(Tuple tuple,Binding binding) {
	    	//1，根据personaId读取分群配置
	    String sqlQuery = "select expression as script from mod_persona where id=? limit 1";
	    logger.debug("try to query candidate persona.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("personaId",tuple.getValueByField("personaId"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    String script = "";
	    if (result != null && result.size() != 0) {
	    		script = result.get(0).get(0).getVal().toString();//获取脚本
	    		
	    		//2，Groovy脚本计算。脚本中引用变量需要通过property+value/score/rank的组合
	        GroovyShell shell = new GroovyShell(binding);
	        Object value = shell.evaluate(script);//返回得分：double
	        try {
	        		double score = Double.parseDouble(""+value);
	        		
	    	        //3，更新到数据库
		    		String sqlUpdate = "update user_persona set scorePersona='_SCORE',modifiedOn=now() where personaId='_PERSONAID' and userKey='_USERKEY'"
				    		.replace("_SCORE", ""+score)
				    		.replace("_PERSONAID", tuple.getValueByField("personaId").toString())
				    		.replace("_USERKEY", tuple.getValueByField("userKey").toString());
		    		logger.debug("try to update persona score to user_persona.[SQL]"+sqlUpdate);
		        jdbcClientAnalyze.executeSql(sqlUpdate); 
	        }catch(Exception ex) {
	        		logger.error("the result cannot be parsed to double value.[result]"+value,ex);
	        }
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate user persona.");
	    }
    }  
    /**
     * 加载用户属性，包括客观评价及主观评价
     * @param userKey
     * @return
     */
    private Binding getUserProfile(String userKey) {
	    	Binding binding = new Binding();
	    	//1，根据userKey从property查询键值对：property,value,score,rank，并准备脚本参数
	    String sqlQuery = "select property,value,score,rank from user_property where userKey=?";
	    logger.debug("try to query user key-values.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("userKey",userKey,Types.VARCHAR));//userKey
	    List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	        for (List<Column> row : result) {//逐行解析并放入参数，每一个key包含三个值，格式为property.value-value,property.score-score,property.rank-rank
	        		binding.setVariable(row.get(0).getVal().toString(), row.get(1).getVal());//可以通过键名得到数值，相当于property.value
	        		binding.setVariable(row.get(0).getVal()+".value", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".score", row.get(2).getVal());
	        		binding.setVariable(row.get(0).getVal()+".rank", row.get(3).getVal());
	        }
	    }else {//如果没有对应的值：do nothing
	    		logger.debug("Cannot find user property values");
	    }
	    //2，加载客观评价：当前客观评价内无type字段，无法按照key加载数据
	    /**
	    sqlQuery = "select type,score,text from user_measure where userKey=?";
	    logger.debug("try to query user measure values.[SQL]"+sqlQuery);
	    result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	        for (List<Column> row : result) {//逐行解析并放入参数
	        		binding.setVariable(row.get(0).getVal().toString(), row.get(1).getVal());//可以通过键名得到数值，相当于property.value
	        		//binding.setVariable(row.get(0).getVal()+".value", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".score", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".text", row.get(2).getVal());
	        }
	    }else {//如果没有对应的值：do nothing
	    		logger.debug("Cannot find user measure values");
	    }	   
	    //**/
	    //3，加载主观评价
	    sqlQuery = "select type,score,text from user_evaluation where userKey=?";
	    logger.debug("try to query user measure values.[SQL]"+sqlQuery);
	    result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	    if (result != null && result.size() != 0) {
	        for (List<Column> row : result) {//逐行解析并放入参数
	        		binding.setVariable(row.get(0).getVal().toString(), row.get(1).getVal());//可以通过键名得到数值，相当于property.value
	        		//binding.setVariable(row.get(0).getVal()+".value", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".score", row.get(1).getVal());
	        		binding.setVariable(row.get(0).getVal()+".text", row.get(2).getVal());
	        }
	    }else {//如果没有对应的值：do nothing
	    		logger.debug("Cannot find user evaluation values");
	    }	    
	    	return binding;
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
