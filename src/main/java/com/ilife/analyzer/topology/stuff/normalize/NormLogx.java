package com.ilife.analyzer.topology.stuff.normalize;

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
import com.ilife.analyzer.spout.stuff.NormSpout;
import com.ilife.analyzer.spout.stuff.PropertySpout;
import com.ilife.analyzer.topology.AbstractTopology;

/**
 * 对指定categoryId、propertyId的数值进行对数归一化。
 * 归一化完成后直接更新ope_performance、分析库value、property表
 * 
 * TODO 调整为正切算法：tan(x)*2/pi()
 * 
 * 算法：修正所有标注值到[1,*）
 * log(1+x)/log(1+max(x))
 * 
 * 步骤：
 * 1，获取logx归一化的属性列表，同时返回categoryId、propertyId
 * 2，根据categoryId、propertyId从ope_performance获取base
 * 3，批量更新，包括ope_performance, value, property
 * 
 * 拓扑：
 * spout---2
 *     	   +---3.1
 *         +---3.2
 *         +---3.3
 */
public class NormLogx extends AbstractTopology {

	    public static void main(String[] args) throws Exception {
	        new NormLogx().execute(args);
	    }

	    @Override
	    public StormTopology getTopology() {
	    	//1，获取待补充marked_value的数值记录
	    	NormSpout propertySpout = new NormSpout(businessConnectionProvider,"logx");

            //2，根据categoryId、propertyId查询min、max
            String sql = "select category_id as categoryId,measure_id as propertyId,log(1+max(marked_value)) as base "
            		+ "from ope_performance "
            		+ "where category_id=? and measure_id=?";
            List<Column> queryParamColumns = Lists.newArrayList(
            		new Column("categoryId", Types.VARCHAR),
            		new Column("propertyId", Types.VARCHAR));
            String[] output_fields = {"categoryId","propertyId","base"};
            Fields outputFields = new Fields(output_fields);
            JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
            JdbcLookupBolt jdbcFindScoreBolt = new JdbcLookupBolt(businessConnectionProvider, sql, jdbcLookupMapper);

            //3.1，更新ope_performance，计算归一化数值
            List<Column> normColumns = Lists.newArrayList(
            		new Column("base", Types.DOUBLE)
            		,new Column("categoryId", Types.VARCHAR)
            		,new Column("propertyId", Types.VARCHAR)
            		);
            JdbcMapper normUpdateMapper = new SimpleJdbcMapper(normColumns);
            JdbcInsertBolt normUpdateBolt = new JdbcInsertBolt(businessConnectionProvider, normUpdateMapper)
                    .withInsertQuery("update ope_performance set normalized_value=log(1+marked_value)/?,update_date=now() where category_id=? and measure_id=?");
            //更新value
            JdbcInsertBolt normUpdateBoltValue = new JdbcInsertBolt(analyzeConnectionProvider, normUpdateMapper)
                    .withInsertQuery("update `value` set score=log(1+marked_value)/?,modifiedOn=now() where categoryId=? and propertyId=?");
            //更新property
            JdbcInsertBolt normUpdateBoltProp = new JdbcInsertBolt(analyzeConnectionProvider, normUpdateMapper)
                    .withInsertQuery("update property set score=log(1+marked_value)/?,modifiedOn=now() where categoryId=? and propertyId=?");
            
            //装配topology
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("norm-logx-spout", propertySpout, 1);
	        builder.setBolt("norm-logx-find", jdbcFindScoreBolt, 1).shuffleGrouping("norm-logx-spout");
	        builder.setBolt("norm-logx-set", normUpdateBolt, 1).shuffleGrouping("norm-logx-find");
	        builder.setBolt("norm-logx-set-value", normUpdateBoltValue, 1).shuffleGrouping("norm-logx-find");
	        builder.setBolt("norm-logx-set-prop", normUpdateBoltProp, 1).shuffleGrouping("norm-logx-find");
	        return builder.createTopology();
	    }
}
