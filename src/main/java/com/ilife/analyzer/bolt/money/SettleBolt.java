
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * 已清算记录逐日结算：今日结算前天之前的所有已清算记录
 * 
 */
public class SettleBolt extends AbstractArangoBolt {
    private static final Logger logger = LoggerFactory.getLogger(SettleBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    String collection = "forge";
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"beneficiary","scheme_item_id","type","date","amount"};
    String[] outfields = inputFields;//将数据继续传递
    
    public SettleBolt(Properties prop,String database,String collection,ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
	 * 1，构建结算凭证号：beneficiary-日期-类型。并查询是否存在此前缀的结算记录得到序号。如果count>0则 序号=count+1
	 * 2，写入结算记录
	 * 3，更新对应清分记录结算状态为settled
     */
    public void execute(Tuple tuple) {
    	 	//完成结算
    	settling(tuple);
    	 	
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
    private void settling(Tuple tuple) {
	    	//1，构建结算凭证号：beneficiary-日期-类型
    		StringBuffer sb = new StringBuffer();
    		//sb.append(tuple.getStringByField("beneficiary"));
    		//sb.append("-");
    		sb.append(tuple.getValueByField("date").toString());
    		sb.append("-");
    		sb.append(tuple.getValueByField("type").toString());
    		String voucher = sb.toString();
    		
    		//查询是否有类似ID。对于锁定情况，有可能出现同一日期统一分润类型的多条结算记录
	    String sqlQuery = "select count(*) as count from mod_settlement where broker_id=? and voucher like ?";
	    logger.debug("try to check voucher count.[SQL]"+sqlQuery);
	    List<Column> queryParams=new ArrayList<Column>();
	    queryParams.add(new Column("broker_id",tuple.getStringByField("beneficiary"),Types.VARCHAR));
	    queryParams.add(new Column("voucher",voucher,Types.VARCHAR));
	    List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
	    int count = Integer.parseInt(result.get(0).get(0).getVal().toString());//得到已有记录数
	    if(count>0) {
	    		sb.append("-");
	    		sb.append(count+1);
	    		voucher = sb.toString();
	    }
	    
		StringBuffer idbuf = new StringBuffer();
		idbuf.append(tuple.getStringByField("beneficiary"));
		idbuf.append("-");
		idbuf.append(voucher);
	    
	    //2，写入结算记录
		String sqlUpdate = "insert into mod_settlement(id,broker_id,voucher,type,amount,status,create_date,update_date) "
				+ "values('__id','_broker_id','_voucher','_type','_amount','settled',now(),now())"
			.replace("__id",Util.md5(idbuf.toString()) )//动态构建md5作为id。通过beneficiary-date-type-#作为唯一识别标志
			.replace("_broker_id", tuple.getStringByField("beneficiary"))
			.replace("_voucher", voucher)
			.replace("_type", tuple.getValueByField("type").toString())
			.replace("_amount", tuple.getValueByField("amount").toString());
		logger.debug("try to insert settlement record.[SQL]"+sqlUpdate);
		jdbcClientBiz.executeSql(sqlUpdate); 	    
		
	    //3，更新清算记录状态
		jdbcClientBiz.executeSql("update mod_clearing set status_settle='settled',update_date=now() where beneficiary='__beneficiary'and scheme_item_id='_scheme_item_id' and status_clear='cleared' and status_settle='settling' and date(create_date)=date('__date')"
        		.replace("__beneficiary", tuple.getValueByField("beneficiary").toString())
        		.replace("_scheme_item_id", tuple.getValueByField("scheme_item_id").toString())
        		.replace("__date", tuple.getValueByField("date").toString()));
		
		//TODO：记录日志
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
