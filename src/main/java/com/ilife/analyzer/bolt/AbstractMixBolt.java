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

package com.ilife.analyzer.bolt;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.apache.storm.Config;
import org.apache.storm.arangodb.common.ArangoDbClient;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoDB;


public abstract class AbstractMixBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMixBolt.class);

    protected OutputCollector collector;
    
    protected Properties conf;
    protected String database;

	protected ArangoDbClient arangoClient;

    protected transient JdbcClient jdbcClient;
    protected Integer queryTimeoutSecs;
    protected ConnectionProvider connectionProvider;

	public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
		this.collector = collector;
		//prepare Arango client
		this.arangoClient = new ArangoDbClient(conf,database);
		
		//prepare jdbc client 
        connectionProvider.prepare();

        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(map.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }

        this.jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
    }

    public AbstractMixBolt(Properties props,String arangoDatabase,ConnectionProvider jdbcConnProvider) {
    		Validate.notEmpty(props.getProperty("arangodb.host"), "host can not be blank or null");
        Validate.notEmpty(props.getProperty("arangodb.port"), "port can not be blank or null");
        Validate.notEmpty(props.getProperty("arangodb.username"), "username can not be blank or null");
        Validate.notEmpty(props.getProperty("arangodb.password"), "password can not be blank or null");
        this.conf = props;
        this.database = arangoDatabase;
        
        Validate.notNull(jdbcConnProvider);
        this.connectionProvider = jdbcConnProvider;
    }

    @Override
    public void cleanup() {
//		this.arangoClient.close();
//		connectionProvider.cleanup();
    }
}
