package com.ilife.analyzer.util;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BeanUtil {
	private static final ObjectMapper mapper = new ObjectMapper();	
	private static final Logger logger = LoggerFactory.getLogger(BeanUtil.class);
    // 将对象转成字符串
    public static String objectToString(Object obj) throws Exception {
        return mapper.writeValueAsString(obj);
    }
    // 将Map转成指定的Bean
    public static Object mapToBean(HashMap map, Class clazz) throws Exception {
    		logger.debug("convert map to object.",map);
        return mapper.readValue(objectToString(map), clazz);
    }
    // 将Bean转成Map
    public static HashMap beanToMap(Object obj) throws Exception {
        return mapper.readValue(objectToString(obj), HashMap.class);
    }
}
