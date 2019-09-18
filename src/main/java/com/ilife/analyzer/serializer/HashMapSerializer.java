package com.ilife.analyzer.serializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HashMapSerializer implements Serializer<HashMap> {
	private static final Logger logger = LoggerFactory.getLogger(HashMapSerializer.class);
	public void close() {
		
	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
		
	}

	public byte[] serialize(String arg0, HashMap map) {
		logger.debug("Try to serialize map.",map);
		byte[] serializedBytes = null;
		ObjectMapper objectMapper = new ObjectMapper();
		//objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		//objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
		try {
			serializedBytes = objectMapper.writeValueAsString(map).getBytes();
		} catch (Exception e) {
			logger.error("Failed while serializing map Object.[map]"+map,e);
		}
		return serializedBytes;
	}

}
