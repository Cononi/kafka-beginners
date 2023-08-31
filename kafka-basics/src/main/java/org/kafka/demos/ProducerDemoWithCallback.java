package org.kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka!");

        // create Producer Properties - 프로듀서 설정 생성
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092"); // 브로커와 연결

        /*
         set producer properties
           프로듀서로 어떤 정보를 전달하면 문자열로 전달된 정보가 Apache Kafka로 전송되기 전에
           key.serializer와 value.serializer를 통해 바이트로 직렬화 된다.
           Producer로 문자열이 들어오면 그 문자열은 Kafka 클라이언트가 제공하는 StringSerializer 클래스를 사용해서
           직렬화 하겠다는 의미다.
         */
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "400");
//        properties.setProperty(partitioner.class, RoundRobinPartitioner.class.getName());

        // create the Producer - 프로듀서 생성
        // Key, Valuse 모두 String Type
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j<10; j++ ){
            // 동시에 여러 메세지 전송처리
            for (int i = 0; i < 30; i++) {
                // create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "hello world " + i);

                // send date - Kafka로 데이터 전송
                // Callback 추가
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // 성공적으로 전송되거나 예외가 발생할 때마다 이 콜백 함수가 실행된다.
                        if (e == null) {
                            // 익셉션이 존재하지 않으면 레코드가 잘 전송됐다는 의미다.
                            log.info("Recevied new metadata \n" +
                                    "Topic :" + metadata.topic() + "\n" +
                                    "Partition :" + metadata.partition() + "\n" +
                                    "Offset :" + metadata.offset() + "\n" +
                                    "Timestamp :" + metadata.timestamp() + "\n");
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
