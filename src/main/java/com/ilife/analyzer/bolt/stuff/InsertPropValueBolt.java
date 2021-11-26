
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
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.ilife.analyzer.util.Util;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 将采集条目的属性值加载到手动标注或字典标注数据库。等待标注。
 * 
 * 根据属性定义，对于手动标注，将候选属性值写入ope_performance；对于字典标注，将候选属性值写入对应的字典表dic_***。对于自动标注或引用标注则不作处理。
 * 
 * 程序逻辑：
 * 1，根据propertyId查询标注类型（同时返回字典表信息）
 * 2.1，如果标注类型是manual，则写入ope_performance
 * 2.2，如果标注类型是dict，则写入对应的字典表
 * 
 * 
 */
public class InsertPropValueBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(InsertPropValueBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    public List<Column> queryParams;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"categoryId","propertyId","value","id"};//前端已经根据categoryId、propertyId、value做了md5，计算了唯一id值，直接使用
    String[] outfields = inputFields;//直接向后流转
    
    public InsertPropValueBolt(ConnectionProvider connectionProvider) {
		this(connectionProvider,connectionProvider);
    }
    
    public InsertPropValueBolt(ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
		this.connectionProviderBiz = connectionProviderBiz;
		this.connectionProviderAnalyze = connectionProviderAnalyze;
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
    }

    public void execute(Tuple tuple) {
    	//根据propertyId从业务库查询属性定义
    	String sqlQuery = "SELECT id,auto_label_type as type, auto_label_dict as dict FROM mod_measure where id='"+tuple.getStringByField("propertyId")+"' limit 1";
        logger.debug("try to query measure.[SQL]"+sqlQuery);
        queryParams.clear();
        List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
        if (result != null && result.size() > 0) {
        	List<Column> row  = result.get(0);//仅有一条记录
        	String type = ""+row.get(1).getVal();
        	String dict = ""+row.get(2).getVal();
        	if("dict".equalsIgnoreCase(type)&&dict.trim().length()>0) {//需要写入字典表
                String sqlInsert = "insert ignore into "+dict+" (id,category,label,create_date,update_date) values(?,?,?,now(),now())";
                String id = Util.md5(tuple.getStringByField("categoryId")+tuple.getStringByField("value"));//同一个分类下值仅能出现一次
            	queryParams.clear();
                queryParams.add(new Column("id", id, Types.VARCHAR));
                queryParams.add(new Column("categoryId", tuple.getStringByField("categoryId"), Types.VARCHAR));
//                queryParams.add(new Column("propertyId", tuple.getStringByField("propertyId"), Types.VARCHAR));//不需要属性ID
                queryParams.add(new Column("label", tuple.getStringByField("value"), Types.VARCHAR));
                logger.info("try to insert manual labeling values into ope_performance.[SQL]"+sqlInsert+"[params]"+queryParams);
                List<List<Column>> items = Lists.newArrayList();
                items.add(queryParams);
                try {
                	jdbcClientBiz.executeInsertQuery(sqlInsert, items);
                }catch(Exception ex) {
                	logger.error("failed insert value to dict table. please check if the dict table exists.[ditc table]"+dict,ex);
                }
        	}else if("manual".equalsIgnoreCase(type)) {//写入ope_performance表
                String sqlInsert = "insert ignore into ope_performance (id,category_id,measure_id,original_value,create_date,update_date) values(?,?,?,?,now(),now())";
                queryParams.clear();
                queryParams.add(new Column("id", tuple.getStringByField("id"), Types.VARCHAR));
                queryParams.add(new Column("categoryId", tuple.getStringByField("categoryId"), Types.VARCHAR));
                queryParams.add(new Column("propertyId", tuple.getStringByField("propertyId"), Types.VARCHAR));
                queryParams.add(new Column("label", tuple.getStringByField("value"), Types.VARCHAR));
                logger.info("try to insert manual labeling values into ope_performance.[SQL]"+sqlInsert+"[params]"+queryParams);
                List<List<Column>> items = Lists.newArrayList();
                items.add(queryParams);
        	    jdbcClientBiz.executeInsertQuery(sqlInsert, items);
        	}else {
        		//do nothing
        	}
        	//处理完成后更新value记录状态为ready
            String sqlUpdate = "update `value` set status='ready',modifiedOn=now() where categoryId=? and propertyId=? and `value`=?";
        	queryParams.clear();
            queryParams.add(new Column("categoryId", tuple.getStringByField("categoryId"), Types.VARCHAR));
            queryParams.add(new Column("propertyId", tuple.getStringByField("propertyId"), Types.VARCHAR));
            queryParams.add(new Column("value", tuple.getStringByField("value"), Types.VARCHAR));
            logger.info("try to update pending value record statuns.[SQL]"+sqlUpdate+"[params]"+queryParams);
            List<List<Column>> items = Lists.newArrayList();
            items.add(queryParams);
    	    jdbcClientAnalyze.executeInsertQuery(sqlUpdate, items);
        }else {//没有配置measure则报错
        	logger.error("no measure found with id.[id]"+tuple.getStringByField("propertyId"));
        }   

        //将数据向后传递
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

    @Override
    public void cleanup() {
//    		connectionProviderBiz.cleanup();
//    		connectionProviderAnalyze.cleanup();
    } 
    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
