package com.ilife.analyzer.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StuffDeseiralizer implements Deserializer<Stuff> {
	private static final Logger logger = LoggerFactory.getLogger(StuffDeseiralizer.class);
	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void configure(Map<String, ?> arg0, boolean arg1) {
		// TODO Auto-generated method stub
		
	}

	public Stuff deserialize(String arg0, byte[] stuffBytes) {
		logger.debug("Try to deserialize Stuff.");
		ObjectMapper mapper = new ObjectMapper();
		Stuff stuff = null;
		try {
			stuff = mapper.readValue(stuffBytes, Stuff.class);
			logger.debug("Finished deserialize Stuff.[Stuff]",stuff);
		} catch (Exception e) {
			logger.error("Failed while serializing Stuff Object.[stuffBytes]"+stuffBytes,e);
		}
		return stuff;
	}

}
