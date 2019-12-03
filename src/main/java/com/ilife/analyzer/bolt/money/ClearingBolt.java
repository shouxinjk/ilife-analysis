
package com.ilife.analyzer.bolt.money;
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
import com.ilife.analyzer.util.Util;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 已成交订单清算
 * 
 */
public class ClearingBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(ClearingBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "forge";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"id","item","amount","order_time","platform","trace_code","commission","broker_id"};
    String[] outfields = inputFields;//将数据继续传递
    
    public ClearingBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
	 * 1，根据订单来源查询得到分润规则
	 * 2，根据分润规则逐条清算，并建立清算记录
	 * 3，更新订单状态为“已清算”，同时更新订单对应的broker_id
	 * 4，提交清算历史到arangodb
     */
    public void execute(Tuple tuple) {
    	 	//完成清算
    	 	clearing(tuple);
    	 	
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
     * 清算    
     */
    private void clearing(Tuple tuple) {
    		//0，获取上级达人、上上级达人
    		Map<String,String> brokers = getBrokers(tuple.getValueByField("broker_id").toString());
    		
	    	//1，根据platform查询所有分润规则。当前不考虑category，均采用default分润规则
	    String sqlQuery = "select a.id as profit_scheme_id,b.id as profit_item_id,b.beneficiary,b.beneficiary_type,b.share from mod_profit_share_scheme a left join mod_profit_share_item b on b.scheme_id=a.id where a.category='default' and a.platform=?";
	    logger.debug("try to query profit share items.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("platform",tuple.getValueByField("platform"),Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    StringBuffer sb = new StringBuffer();
	    if (result != null && result.size() != 0) {
	    		//逐条产生清分记录
            for (List<Column> row : result) {
               String beneficiary = "";
               String beneficiaryType = "";
               String profit_scheme_id = "";
               String profit_item_id = "";
               double share = 0;
               for(Column column : row) {//获取分润详细设置
                   if("beneficiary".equalsIgnoreCase(column.getColumnName())) {
                	   		beneficiary = column.getVal().toString();
                   }else if("beneficiary_type".equalsIgnoreCase(column.getColumnName())) {
                	   	beneficiaryType = column.getVal().toString();
                   }else if("share".equalsIgnoreCase(column.getColumnName())) {
	                	   try {
			      	   		share = Double.parseDouble(column.getVal().toString());
	                	   }catch(Exception ex) {
	                		   logger.error("\n======failed to parse share.=======\n",ex);
	                		   share = 0;
	                	   }
                   }else if("profit_scheme_id".equalsIgnoreCase(column.getColumnName())) {
                	   		profit_scheme_id = column.getVal().toString();
                   }else if("profit_item_id".equalsIgnoreCase(column.getColumnName())) {
	                	  	profit_item_id = column.getVal().toString();
	               }
               }
               
               //开始计算
               String status = "done";
               String person = brokers.get("broker");//默认为当前broker
               double amount = share * tuple.getDoubleByField("commission")/100;
               DecimalFormat df = new DecimalFormat("#.00");//保留两位小数
               String amountStr = df.format(amount);

               if("team".equalsIgnoreCase(beneficiaryType)) { //beneficiaryType==team
            	   		status = "pending";//需要二次清分
            	   		person = beneficiary;//对于团队绩效，直接使用团队标记
               }else{//beneficiaryType==person
	            	   if("platform".equalsIgnoreCase(beneficiary)) { //如果是平台收入，则作为特殊情况处理
	            		   person = "platform";//直接使用平台标记：是一个特殊的达人账户
	               }else{//具体个人分润：推广达人、上级、上上级
	            	   		person = brokers.get(beneficiary)==null?"null-"+beneficiary:brokers.get(beneficiary);//使用真实的ID填充
	               }
            }
               
	             //2,写入清分记录
	               //采用order_id+scheme_id+scheme_item_id+beneficiary+beneficiary_type作为id
				String idStr = tuple.getValueByField("id").toString()+profit_scheme_id+profit_item_id+beneficiaryType+beneficiary+person;
				String sqlUpdate = "insert into mod_clearing(id,platform,order_id,order_time,item,amount_order,amount_commission,amount_profit,scheme_id,scheme_item_id,beneficiary,beneficiary_type,status) "
						+ "values('__id','_platform','_order_id','_order_time','__item','_amount_order','_amount_commission','_amount_profit','_scheme_id','_scheme_item_id','__beneficiary','_beneficiary_type','_status')"
					.replace("__id",Util.md5(idStr) )//动态构建md5作为id
					.replace("_platform", tuple.getStringByField("platform"))
					.replace("_order_id", tuple.getStringByField("id"))
					.replace("_order_time", tuple.getValueByField("order_time").toString())//date time
					.replace("__item", tuple.getStringByField("item"))
					.replace("_amount_order", ""+tuple.getDoubleByField("amount"))
					.replace("_amount_commission", ""+tuple.getDoubleByField("commission"))
					.replace("_amount_profit", amountStr)
					.replace("_scheme_id", profit_scheme_id)
					.replace("_scheme_item_id", profit_item_id)
					.replace("__beneficiary", person)
					.replace("_beneficiary_type", beneficiaryType)
					.replace("_status", status);
				logger.debug("try to insert clearing record.[SQL]"+sqlUpdate);
				jdbcClientBiz.executeSql(sqlUpdate); 
            }
	        
            //3,更新原始订单状态为已清分
            String sqlUpdate = "update mod_order set status='cleared' where id='__id'"
				.replace("__id",tuple.getStringByField("id"));
			logger.debug("try to update order status.[SQL]"+sqlUpdate);
			jdbcClientBiz.executeSql(sqlUpdate); 
	       
	        //4，TODO：记录日志或同步到Arango
            /**
	        if(tuple.getBooleanByField("featured"))  {
	        		syncText(tuple.getValueByField("_key").toString(),tuple.getValueByField("type").toString(),sb.toString());
	        }
	        //**/
	    }else {//could not happend. 如果没有对应的值：do nothing
	    		logger.debug("Failed to evaluate satisfication.");
	    }
    }   

    /**
     * 查询当前达人的上级达人、上上级达人。如果缺失，则以本人填充    
     */
    private Map<String,String> getBrokers(String broker_id) {
    		Map<String,String> brokers = new HashMap<String,String>();
    		brokers.put("broker", broker_id);
	    	//1，查询上级达人
	    String parent_id = getParentBroker(broker_id);
	    brokers.put("parent", parent_id);
	    	//2，查询上上级达人
	    String grandpa_id = getParentBroker(parent_id);
	    brokers.put("grandpa", grandpa_id);
	    
	    logger.error("\n\n====broker list====\n\n",brokers);
	    
	    return brokers;
    }   
    
    private String getParentBroker(String broker_id) {
	    String sqlQuery = "select parent_id from mod_broker where id=? limit 1";
	    logger.debug("try to query parent broker.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("id",broker_id,Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    if (result != null && result.size() > 0) {
	        String parent_id = result.get(0).get(0).getVal().toString();//上级达人ID
	        if(parent_id == null || parent_id.trim().length()==0)
	        		return broker_id;//如果parent 为空则返回达人自己
	        return parent_id;
	    }else {//如果没有上级达人则返回自己
	    		logger.debug("has no parent broker.");
	    		return broker_id;
	    }
    }
    
    private void syncText(String itemKey,String type, String text) {
		//执行数据更新
		BaseDocument doc = new BaseDocument();
		doc.setKey(itemKey);
	
		//Map<String,Object> evaluate = new HashMap<String,Object>();
		//evaluate.put(type, text);
		//doc.getProperties().put("evaluate", evaluate);
		doc.getProperties().put(type, text);//直接写入属性
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
