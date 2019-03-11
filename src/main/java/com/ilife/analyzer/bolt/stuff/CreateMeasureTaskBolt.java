
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 建立客观评价任务，包括两个类别：维度-属性评价、维度-维度评价
 * 
 * 根据itemKey、Category建立measure-property任务记录，后续进行叶子节点维度评估
 * 1，根据category从业务库内查询dimension-measure记录
 * 2，将itemKey、category、dimension、measure、weight属性关系写入分析库
 * 3，根据category从业务库内查询dimension-dimension记录
 * 4，将itemKey、category、dimension、dimension、weight属性关系写入分析库
 * 
 */
public class CreateMeasureTaskBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(CreateMeasureTaskBolt.class);
    protected OutputCollector collector;
    
    Integer queryTimeoutSecs;
    public List<Column> queryParams;
    
    protected transient JdbcClient jdbcClientBiz;
    protected transient JdbcClient jdbcClientAnalyze;
    protected ConnectionProvider connectionProviderBiz;
    protected ConnectionProvider connectionProviderAnalyze;
    
    String[] inputFields = {"_key","category"};//输入字段包含itemkey和category
    String[] outfields = {"itemKey","category"};//输出字段包含itemKey和Category，便于创建后续分析任务
    
    public CreateMeasureTaskBolt(ConnectionProvider connectionProvider) {
		this(connectionProvider,connectionProvider);
    }
    
    public CreateMeasureTaskBolt(ConnectionProvider connectionProviderBiz,ConnectionProvider connectionProviderAnalyze) {
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
    	 	String category = tuple.getStringByField("category");//获取category字段
    	 	queryParams.clear();
        queryParams.add(new Column("category", category, Types.VARCHAR));
        
	    	//查询measure-measure记录
    		//1，查询业务库得到measure-measure记录
        	//select dim.parent_id as parent,dim.id as dimension, dim.weight as weight,dim.parent_ids as depth from mod_category cat,mod_item_dimension dim where cat.name=? and cat.id=dim.category
        		//2，写入分析库
        	//insert ignore into measure (itemKey,parent,dimension,weight,status,priority,createdOn,modifiedOn) values()
        		String sqlQuery = "select dim.parent_id as parent,dim.id as dimension, dim.weight as weight,dim.parent_ids as depth from mod_item_category cat,mod_item_dimension dim where cat.name=? and cat.id=dim.category";
            logger.info("try to query pending measure-dimension.[SQL]"+sqlQuery+"[param]"+queryParams);
            String sqlInsert = "insert ignore into measure (itemKey,parent,dimension,weight,status,priority,revision,createdOn,modifiedOn) values(?,?,?,?,'pending',?,1,now(),now())";
            List<List<Column>> items = new ArrayList<List<Column>>();
            List<List<Column>> result = jdbcClientBiz.select(sqlQuery,queryParams);
            if (result != null && result.size() != 0) {
                for (List<Column> row : result) {
                		List<Column> item = new ArrayList<Column>();
                		item.add(new Column("itemKey",tuple.getValueByField("_key"),Types.VARCHAR));//itemKey
                		int priority = 400;
                		for(Column column:row) {//dimension，measure，weight
                			if("depth".equalsIgnoreCase(column.getColumnName())) {
                				String[] depth = column.getVal().toString().split(",");
                				priority = 400+depth.length;//按照层级，叶子节点优先级更高
                			}else {//作为插入参数
                				item.add(new Column(column.getColumnName(), column.getVal(), column.getSqlType()));
                			}
                		}
                		item.add(new Column("priority",priority,Types.INTEGER));//priority:手动设置为900
                		items.add(item);
                }
    	    		//写入分析库
            logger.info("try to insert pending measure-dimension tasks.[SQL]"+sqlInsert+"[items]"+items);
    	    		jdbcClientAnalyze.executeInsertQuery(sqlInsert, items);	    		
        }else {//没有配置measure-measure的情况：do nothing
        		logger.debug("no more measure-dimension tasks");
        }  
        
        //查询measure-property记录
    		//1，查询业务库得到measure-property记录
    	//select int.dimension_id as dimension,int.measure_id as measure, int.weight as weight from mod_category cat,int_item_dimension_measure int where cat.name=? and cat.id=int.category
    		//2，写入分析库
    	//insert ignore into measureproperty (itemKey,dimension,property,weight,status,priority,createdOn,modifiedOn) values()
        sqlQuery = "select inter.dimension_id as dimension,inter.measure_id as measure, inter.weight as weight from mod_item_category cat,int_item_dimension_measure inter where cat.name=? and cat.id=inter.category";
        logger.debug("try to query pending measure-property.[SQL]"+sqlQuery+"[param]"+queryParams);
        sqlInsert = "insert ignore into measure_property (itemKey,dimension,property,weight,status,priority,revision,createdOn,modifiedOn) values(?,?,?,?,'pending',?,1,now(),now())";
        items = new ArrayList<List<Column>>();
        result = jdbcClientBiz.select(sqlQuery,queryParams);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
            		List<Column> item = new ArrayList<Column>();
            		item.add(new Column("itemKey",tuple.getValueByField("_key"),Types.VARCHAR));//itemKey
            		String dimension = "";
            		for(Column column:row) {//dimension，measure，weight
        				item.add(new Column(column.getColumnName(), column.getVal(), column.getSqlType()));
        				if("dimension".equalsIgnoreCase(column.getColumnName()))
        					dimension = column.getVal().toString();
            		}
            		item.add(new Column("priority",900,Types.INTEGER));//注意：设置priority不会被引用，由于计算时只从measure内读取任务，该优先级需要进行更新
            		items.add(item);
    	    			//更新对应叶子measure记录的优先级
            		String updatePrioritySql = "update measure set priority='900' where itemKey='_ITEMKEY' and dimension='_DIMENSION'"
						            		.replace("_ITEMKEY", tuple.getValueByField("_key").toString())
						            		.replace("_DIMENSION", dimension);
            		jdbcClientAnalyze.executeSql(updatePrioritySql);
            }
	    		//写入分析库
            logger.info("try to insert pending measure_property tasks.[SQL]"+sqlInsert+"[items]"+items);
	    		jdbcClientAnalyze.executeInsertQuery(sqlInsert, items);
        }else {//没有配置measure-property的情况：do nothing
	    		logger.debug("No pending measure-property tasks");
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

    @Override
    public void cleanup() {
    		connectionProviderBiz.cleanup();
    		connectionProviderAnalyze.cleanup();
    } 
    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    		outputFieldsDeclarer.declare(new Fields(outfields));
    }
}
