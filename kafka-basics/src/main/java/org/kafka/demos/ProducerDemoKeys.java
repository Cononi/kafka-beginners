package org.kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka!");

        // create Producer Properties - 프로듀서 설정 생성
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092"); // 브로커와 연결
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer - 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world " + i;
                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send date - Kafka로 데이터 전송
                producer.send(producerRecord, new Callback() {
                    // Callback 추가
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // 성공적으로 전송되거나 예외가 발생할 때마다 이 콜백 함수가 실행된다.
                        if (e == null) {
                            // 익셉션이 존재하지 않으면 레코드가 잘 전송됐다는 의미다.
                            log.info("Key: " + key + " | Partition :" + metadata.partition());
                        } else {
                            // 생산 중에 오류가 발생
                            log.error("Error while producting", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // flush and close the producer - 프로듀서에 전송된 내용을 반영하고 종료
        producer.flush();
        producer.close();
    }
}
