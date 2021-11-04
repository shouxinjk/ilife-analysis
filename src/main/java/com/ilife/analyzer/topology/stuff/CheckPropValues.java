package com.ilife.analyzer.topology.stuff;

import java.sql.Types;
import java.util.List;

import org.apache.storm.arangodb.bolt.ArangoInsertBolt;
import org.apache.storm.arangodb.bolt.ArangoLookupBolt;
import org.apache.storm.arangodb.bolt.ArangoUpdateBolt;
import org.apache.storm.arangodb.common.QueryFilterCreator;
import org.apache.storm.arangodb.common.SimpleQueryFilterCreator;
import org.apache.storm.arangodb.common.mapper.ArangoLookupMapper;
import org.apache.storm.arangodb.common.mapper.ArangoMapper;
import org.apache.storm.arangodb.common.mapper.ArangoUpdateMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoLookupMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoMapper;
import org.apache.storm.arangodb.common.mapper.SimpleArangoUpdateMapper;
import org.apache.storm.arangodb.spout.ArangoSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.ilife.analyzer.spout.stuff.CategoryIdSpout;
import com.ilife.analyzer.spout.stuff.PropValuesSpout;
import com.ilife.analyzer.spout.stuff.PropertyIdSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 将已经完成目录映射及属性映射的数值同步到业务库，用于标注。
 * 
 * 本任务内仅关注指定categoryId下的属性，不关注从上级目录继承的属性
 * 
 * 处理逻辑：
 * 1，从分析库 value 表内读取状态为pending、且propertyId不为空的记录。如果记录为0 ，则统一更新由propertyId的记录状态为pending，开启下一轮分析
 * 2，将对应数据写入分析库 ope_performance 表内。根据 propertyId、value进行唯一性校验
 *
 */
public class CheckPropValues extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new CheckPropValues().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
    		//1，获取待同步数值记录
    		PropValuesSpout propValesSpout = new PropValuesSpout(analyzeConnectionProvider);
    		
    		//2，将查询出的value更新写入业务库ope_performance
            //注意：采用insert on duplicate key方式，如果重复则只更新时间戳。唯一性标记为propertyId、value值
            List<Column> bizPerformanceColumns = Lists.newArrayList(
            		new Column("id", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR),
            		new Column("value", Types.VARCHAR));
            JdbcMapper insertPerformnceMapper = new SimpleJdbcMapper(bizPerformanceColumns);
            JdbcInsertBolt jdbcUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, insertPerformnceMapper)
                    .withInsertQuery("insert into ope_performance(id,measure_id,original_value,create_date,update_date) "
                    		+ "values(?,?,?,now(),now()) on duplicate key update update_date=now()");	
            
            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("check_prop_values", propValesSpout, 1);
	        builder.setBolt("sync_performance_value", jdbcUpdateBolt, 1).shuffleGrouping("check_prop_values");
	        return builder.createTopology();
	    }
}
