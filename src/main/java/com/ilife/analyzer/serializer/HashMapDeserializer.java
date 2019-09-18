package com.ilife.analyzer.serializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HashMapDeserializer implements Deserializer<HashMap> {
	private static final Logger logger = LoggerFactory.getLogger(HashMapDeserializer.class);
	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	public HashMap deserialize(String arg0, byte[] bytes) {
		logger.debug("Try to deserialize map.");
		ObjectMapper mapper = new ObjectMapper();
		HashMap map = null;
		try {
			map = mapper.readValue(bytes, HashMap.class);
			logger.debug("Finished deserialize map.[map]",map);
		} catch (Exception e) {
			logger.error("Failed while serializing map Object.[bytes]"+bytes,e);
		}
		return map;
	}

}
