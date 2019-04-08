
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
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.entity.BaseDocument;
import com.google.gson.Gson;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 建立个性化Filter。根据用户属性组建附加Filter，用于精准推荐。
 * 1，构建bool query：jdbcbolt从analyze:user_query查询得到，并按照must、must not、filter、should构建布尔查询，输入userKey
 * 2，构建function score(step 1)：jdbcbolt从analyze:user_persona查询得到最匹配persona，如果没有则使用默认值。输出userKey，personaId
 * 3，构建function score(step 2)：jdbcbolt从business:mod_persona查询得到vals，构建8个gauss衰减函数，输出
 * 4，构建关注度等固定function：直接硬编码
 * 5，更新到user_user:arangobolt直接更新query字段
 * 
 * Json:
 * function_score:{
 * 	query:{
 * 		bool:{
 * 			must:[],
 * 			must_not:[],
 * 			should:[],
 * 			filter:[]
 * 		}
 * 	},
 * 	functions:[],
 * 	boost:"",
 * 	score_mode:"multiply",
 * 	boost_mode:"multiply"
 * }
 * 
 */
public class BuildFilterBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(BuildFilterBolt.class);
    protected OutputCollector collector;
    String collection = "user_users";
    
    Integer queryTimeoutSecs;
    private List<Column> queryParams;
    
    private List<Map<String,Object>> boolMust;
    private List<Map<String,Object>> boolMustNot;
    private List<Map<String,Object>> boolShould;
    private List<Map<String,Object>> boolFilter;
    private List<Map<String,Object>> scoreFunctions;
    private List<Map<String,Object>> esQuery;
    
    double gaussDecay = 0.25;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"userKey"};//输入字段包含userKey
    String[] outfields = {"userKey"};//输出字段包含userKey，便于创建后续分析任务
    
    public BuildFilterBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
    		super(prop,database);	
    		this.connectionProviderBiz = connectionProviderBiz;
    		this.connectionProviderAnalyze = connectionProviderAnalyze;
    		this.collection = collection;
        this.queryParams = new ArrayList<Column>();
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
    
    private void prepareQuery() {
        this.boolMust = new ArrayList<Map<String,Object>>();
        this.boolMustNot = new ArrayList<Map<String,Object>>();
        this.boolShould = new ArrayList<Map<String,Object>>();
        this.boolFilter = new ArrayList<Map<String,Object>>();
        this.scoreFunctions = new ArrayList<Map<String,Object>>();
        this.esQuery = new ArrayList<Map<String,Object>>();    	
    }

    public void execute(Tuple tuple) {
    		//!!!注意：此处未按照单个用户分别存储Filter，需要在执行前进行控制。否则会导致数据交叉 Important!!!
        prepareQuery();//每次执行前处理对象
    		//1，构建bool query
    		buildBoolQuery(tuple);
    		//2，查询persona
    	 	queryParams.clear();
    	 	queryParams.add(new Column("userKey",tuple.getValueByField("userKey"),Types.VARCHAR));
    		String sqlQuery = "select personaId from user_persona where userKey=? order by scoreHierarchy desc,ScorePhase desc,scorePersona desc limit 1";
         logger.debug("try to query matched user_persona.[SQL]"+sqlQuery);
         List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);//从分析库查询符合的persona
         if (result != null && result.size() != 0) {
        	 	String personaId = result.get(0).get(0).getVal().toString();//获取personaId
        	 	//3，从业务库查询对应persona的vals配置
        	 	scoreVals(tuple,personaId);
         }else {//没有配置匹配的Persona：do nothing
         	logger.error("Cannot find matched persona.[userKey]"+tuple.getStringByField("userKey"));
         }   
 
 	 	//4，从分析库查询对应的成本计算结果
         scoreCost(tuple,"economy","x");
         scoreCost(tuple,"society","y");
         scoreCost(tuple,"culture","z");
         
        //5，构建脚本评分
         buildScriptFunctions();
         
        //6，汇总并写入ArangoDB
         Map<String,Object> boolObj = new HashMap<String,Object>();
         if(boolMust.size()>0)boolObj.put("must", boolMust);
         if(boolMustNot.size()>0)boolObj.put("must_not", boolMustNot);
         if(boolShould.size()>0)boolObj.put("should", boolShould);
         if(boolFilter.size()>0)boolObj.put("filter", boolFilter);
         
         Map<String,Object> queryObj = new HashMap<String,Object>();
         queryObj.put("bool", boolObj);
         
         Map<String,Object> functionScoreObj = new HashMap<String,Object>();
         functionScoreObj.put("query", queryObj);
         functionScoreObj.put("functions", scoreFunctions);
         functionScoreObj.put("boost", "2");
         functionScoreObj.put("score_mode", "multiply");
         functionScoreObj.put("boost_mode", "multiply");
         
         Map<String,Object> wrapper = new HashMap<String,Object>();
         wrapper.put("function_score", functionScoreObj);
         
         logger.debug("user query function.",wrapper);
         System.err.println(wrapper);
         
         //写入arangodb
 		BaseDocument doc = new BaseDocument();
 		doc.setKey(tuple.getStringByField("userKey"));
 		doc.getProperties().put("query", wrapper);
 		arangoClient.update("user_users", doc.getKey(), doc);            
         
        //将userKey、category向后传递
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
    
    private void scoreVals(Tuple tuple,String personaId) {
	 	queryParams.clear();
	 	queryParams.add(new Column("id",personaId,Types.VARCHAR));
	 	String sqlQuery  = "select alpha as a,beta as b,gamma as c,delte as d,epsilon as e from mod_persona where id=? limit 1";
	 	logger.debug("try to query vals from mod_persona.[SQL]"+sqlQuery);
	 	List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);//从业务库查询符合的persona详细配置   
		if (result != null && result.size() != 0) {
			List<Column> columns = result.get(0);
			for(Column column:columns) {
				double origin = 0.5;
				try {
					origin = Double.parseDouble(column.getVal()+"");
				}catch(Exception ex) {
					logger.error("cannot parse vals.",ex);
				}
				buildGaussFunctions(column.getColumnName(),origin,gaussDecay);
			}
		 }else {//没有配置匹配的Persona：do nothing
			 logger.error("Cannot find matched persona.[userKey]"+tuple.getStringByField("userKey"));
		 } 	 	
    }
    
    private void scoreCost(Tuple tuple, String type,String field) {
	 	queryParams.clear();
	 	queryParams.add(new Column("userKey",tuple.getStringByField("userKey"),Types.VARCHAR));
	 	queryParams.add(new Column("type",type,Types.VARCHAR));
	 	String sqlQuery  = "select score from user_evaluation where userKey=? and type=? limit 1";
	 	logger.debug("try to query evaluted params from user_evaluation.[SQL]"+sqlQuery);
	 	List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);//从分析库查询对应的结算结果   
		if (result != null && result.size() != 0) {
			List<Column> columns = result.get(0);
			double origin = 0.5;
			try {
				origin = Double.parseDouble(columns.get(0).getVal()+"");
			}catch(Exception ex) {
				logger.error("cannot parse cost value.",ex);
			}
			buildGaussFunctions(field,origin,gaussDecay);

		 }else {//没有配置匹配的Persona：do nothing
			 logger.error("Cannot find typed value.[userKey]"+tuple.getStringByField("userKey")+"[type]"+type);
		 } 	 	    	
    }
    
    /**
     * 查询user_query并组装bool query
     */
    private void buildBoolQuery(Tuple tuple) {
	    	queryParams.clear();
	    	queryParams.add(new Column("userKey",tuple.getValueByField("userKey"),Types.VARCHAR));
		String sqlQuery = "select category,type,field,text from user_query where category!='' and category is not null and field is not null and field!='' and text is not null and text!='' and userKey=?";
	     logger.debug("try to query user_query.[SQL]"+sqlQuery);
	     List<List<Column>> result = jdbcClientAnalyze.select(sqlQuery,queryParams);
	     if (result != null && result.size() != 0) {
	         for (List<Column> row : result) {
	        	 	Map<String,String> fieldQuery = new HashMap<String,String>();
	        	 	fieldQuery.put(""+row.get(2).getVal(), ""+row.get(3).getVal());//title:亲子
	        	 	Map<String,Object> boolQuery = new HashMap<String,Object>();
	        	 	boolQuery.put(""+row.get(1).getVal(), fieldQuery);//match:{title:亲子}
	        	 	if("must".equalsIgnoreCase(row.get(0).getVal().toString())) {//must:[]
	        	 		boolMust.add(boolQuery);
	        	 	}else if("must_not".equalsIgnoreCase(row.get(0).getVal().toString())) {//must_not:[]
	        	 		boolMustNot.add(boolQuery);
	        	 	}else if("should".equalsIgnoreCase(row.get(0).getVal().toString())) {//should:[]
	        	 		boolShould.add(boolQuery);
	        	 	}else if("filter".equalsIgnoreCase(row.get(0).getVal().toString())) {//filter:[]
	        	 		boolFilter.add(boolQuery);
	        	 	}else {//error category
	        	 		logger.debug("ignore error bool query type.[type]"+row.get(0).getVal());
	        	 	}
	         }	    		
	     }else {
	     		logger.debug("no more bool query items.");
	     }
    }
    
    /**
     * 构建gauss衰减函数
     {
	    "gauss":{
	      "price.sale":{
	        "origin":"600",
	        "scale":"50"
	      }
	    }
	  }
     */
    private void buildGaussFunctions(String field,double origin,double scale) {
    		Map<String,Object> obj = new HashMap<String,Object>();
    		obj.put("origin", origin);
    		obj.put("scale", scale);
    		Map<String,Object> fieldObj = new HashMap<String,Object>();
    		fieldObj.put(field, obj);
    		Map<String,Object> gaussObj = new HashMap<String,Object>();
    		gaussObj.put("gauss", fieldObj);
    		scoreFunctions.add(gaussObj);
    }
    
    /**
     * 构建脚本函数：当前通过硬编码写死
     {
        "script_score":{
          "script" : {
              "params": {
                  "a": 5,
                  "b": 0.3,
                  "c": 0.7
              },
              "source": "Math.pow(params.a, params.b*doc['rank.count'].value+params.c*doc['rank.score'].value)"
          }
        }
      }
     */
    private void buildScriptFunctions() {
    		Map<String,Object> params = new HashMap<String,Object>();
    		params.put("a", 5);
    		params.put("b", 2);
    		params.put("c", 3);
    		Map<String,Object> script = new HashMap<String,Object>();
    		script.put("params", params);
    		script.put("source", "Math.pow(params.a, params.b*doc['rank.count'].value+params.c*doc['rank.score'].value)");
    		Map<String,Object> script_score = new HashMap<String,Object>();
    		script_score.put("script", script);
    		Map<String,Object> wrapper = new HashMap<String,Object>();
    		wrapper.put("script_score", script_score);
    		scoreFunctions.add(wrapper);
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
