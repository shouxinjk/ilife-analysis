/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ilife.analyzer.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.LocalCluster;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractTopology {

    protected ConnectionProvider analyzeConnectionProvider;
    protected ConnectionProvider businessConnectionProvider;
    protected Properties props=new Properties();
    private static final Logger logger = Logger.getLogger(AbstractTopology.class);
    		
    protected static final String JDBC_CONF = "jdbc.conf";
    
    public void execute(String[] args) throws Exception {
    	//here we load configurations from properties file
        props.load(AbstractTopology.class.getClassLoader().getResourceAsStream("ilife.properties"));
        
        //prepare storm configuration
        Config config = new Config();
        if("debug".equalsIgnoreCase(props.getProperty("common.mode", "production")))
        		config.setDebug(true); 
        else
        		config.setDebug(false);
        
        //prepare JDBC configuration
        prepareConnectionProvider("business");//业务数据库
        prepareConnectionProvider("analyze");//分析数据库
        
        //prepare ArangoDB configuration
        
        //prepare Kafka configuration
        //refer to ilife.properties
        
        config.setNumAckers(0);//we don't need ack
//        System.out.println(config);
        
        //submit topology
        String topoName = props.getProperty("common.topology.name", "ilifeAnalyzeTopology");
        if (args != null && args.length > 0) {
        	config.setNumWorkers(1); 
            StormSubmitter.submitTopology(args[0], config, getTopology());
        } else {
        	//TODO to set TOPOLOGY_ACKERS to 0 to disable ack
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topoName, config, getTopology());
            Thread.sleep(30000);
            cluster.killTopology(topoName);
            cluster.shutdown();
        }
    }
    
    public void prepareConnectionProvider(String type) {
        //prepare JDBC configuration
        Map jdbcConfigMap = Maps.newHashMap();
        jdbcConfigMap.put("dataSourceClassName", props.getProperty("mysql.TYPE.dataSource.className".replace("TYPE", type)));//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
        jdbcConfigMap.put("dataSource.url", props.getProperty("mysql.TYPE.url".replace("TYPE", type)));//jdbc:mysql://localhost/test
        jdbcConfigMap.put("dataSource.user", props.getProperty("mysql.TYPE.user".replace("TYPE", type)));//root
        jdbcConfigMap.put("dataSource.password", props.getProperty("mysql.TYPE.password".replace("TYPE", type)));//password
//        try{
//	        jdbcConfigMap.put("minimumIdle", Integer.parseInt(props.getProperty("mysql.minimumIdle")));//minIdle
//        }catch(Exception ex){
//        		jdbcConfigMap.put("minimumIdle", 2);
//	        logger.error("Cannot read minimumIdle from properties.",ex);
//        }
        try{
	        jdbcConfigMap.put("maximumPoolSize", Integer.parseInt(props.getProperty("mysql.maximumPoolSize")));//maxPoolSize
        }catch(Exception ex){
	        	jdbcConfigMap.put("maximumPoolSize", 5);
	        	logger.error("Cannot read maximumPoolSize from properties.",ex);
        }
  //config.put(JDBC_CONF, jdbcConfigMap); 
        if("business".equalsIgnoreCase(type))
        		this.analyzeConnectionProvider = new HikariCPConnectionProvider(jdbcConfigMap); 
        else if("analyze".equalsIgnoreCase(type))
    			this.analyzeConnectionProvider = new HikariCPConnectionProvider(jdbcConfigMap); 
        else
        		logger.error("Wrong connection type. TYPE must be business or analyze.");
    }

    public abstract StormTopology getTopology();

}
