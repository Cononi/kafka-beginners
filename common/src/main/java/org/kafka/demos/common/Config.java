package org.kafka.demos.common;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
public class Config {
    // 카프카 서버
    static final String bootstrapServers = "127.0.0.1:9092";

    // 카프카 프로듀서 프로퍼티
    public static Properties kafkaProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092"); // 브로커와 연결

        properties.setProperty("key.serializer",StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        return properties;
    }
}