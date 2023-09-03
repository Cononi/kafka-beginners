package org.kafka.demos.common;

import org.apache.kafka.clients.producer.ProducerConfig;
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
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"); // 브로커와 연결

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2.8 이전일 떄
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        // 필요 하다면 설정
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return properties;
    }
}
