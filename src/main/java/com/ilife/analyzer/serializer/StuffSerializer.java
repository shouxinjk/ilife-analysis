package com.ilife.analyzer.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StuffSerializer implements Serializer<Stuff> {
	private static final Logger logger = LoggerFactory.getLogger(StuffSerializer.class);
	public void close() {
		
	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
		
	}

	public byte[] serialize(String arg0, Stuff stuff) {
		logger.debug("Try to serialize Stuff.",stuff);
		byte[] serializedBytes = null;
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
		try {
			serializedBytes = objectMapper.writeValueAsString(stuff).getBytes();
		} catch (Exception e) {
			logger.error("Failed while serializing Stuff Object.[Stuff]"+stuff,e);
		}
		return serializedBytes;
	}
}
