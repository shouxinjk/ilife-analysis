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
package com.ilife.analyzer.topology.person;

import org.apache.flink.storm.api.FlinkTopology;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.ilife.analyzer.topology.AbstractTopology;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

import java.sql.Types;
import java.util.List;


public class OnChangePersona extends AbstractTopology {
    private static final String SPOUT_KAFKA_CHANGE_PERSONA = "SPOUT_KAFKA_CHANGE_PERSONA".toLowerCase();
    private static final String BOLT_CHANGE_PERSONA_NEED = "BOLT_CHANGE_PERSONA_NEED".toLowerCase();
    private static final String BOLT_CHANGE_PERSONA_OCCASION = "BOLT_CHANGE_PERSONA_OCCASION".toLowerCase();
    private static final String BOLT_CHANGE_PERSONA_DEF = "BOLT_CHANGE_PERSONA_DEF".toLowerCase();
    
    public static void main(String[] args) throws Exception {
        new OnChangePersona().execute(args);
    }

    @Override
    public FlinkTopology getTopology() {
    		//1，从persona topic接收kafka消息。消息包含修改后的内容，格式为
    		// {persona:{old:{},new:{}},needs:{old:{},new:{}},occasions:{old:{},new:{}} }
    		String endpoint = props.getProperty("kafka.bootstrap.servers");
    		String topic = props.getProperty("kafka.topics");

    		
//    		KafkaSpout kafkaSpout = new KafkaSpout(KafkaSpoutConfig.builder(endpoint, topic).build());

    		//2，如果needs新旧值不同，则更新Persona-Scenario、Person-Scenario，
    		//注意使用type=need过滤
    		
    		//3，如果occasion新旧值不同，则更新Persona-Scenario、Person-Scenario，
    		//注意使用type=occasion过滤，且仅处理删除的occasion定义，新增occasion通过自动任务触发
    		
    		//4，更新Persona，如果Persona判定规则新旧值不同，则升级Person版本，触发Person是否与现有persona匹配分析
    		
    		//5，更新所有Person版本，升级version，同时更新build为0
    		
    		//6,其他分析程序会自动获取数据进行计算

        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout(SPOUT_KAFKA_CHANGE_PERSONA, kafkaSpout, 1);
//        builder.setBolt(SQL_DELETE_UNMATCH_CHECKUP_ITEM, jdbcCleanCheckupItemBolt, 1).shuffleGrouping(PENDING_CHECKUP_ITEM_SPOUT);
        return FlinkTopology.createTopology(builder);
    }
}
