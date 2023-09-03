package org.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        String groupId = "my-java-application";
        String topic = "demo_java";

        log.info("I am a Kafka Consumer!");

        // create Producer Properties - 프로듀서 설정 생성
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"); // 브로커와 연결


        // create consumer configs - 역직렬화
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer - 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a Topic - 토픽 구독
//        consumer.subscribe(Arrays.asList("topic1","topic2"));
        consumer.subscribe(Arrays.asList(topic));

        // poll for data - 토픽으로 부터 데이터 받아오기
        while (true) {
            log.info("Polling");

            // 1000 밀리초동안 대기
            ConsumerRecords<String, String> recodes =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String,String> record : recodes){
                log.info("key: " + record.key() + ", value: " + record.value());
                log.info("partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
