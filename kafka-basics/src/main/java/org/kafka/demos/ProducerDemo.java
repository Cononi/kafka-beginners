package org.kafka.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

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


        // create Producer Properties - 만일 외부 보안 연결이 필요하다면? 아래와 같이 필요 설정에 따라 작성
        //        properties.setProperty("security.protocol","SASL_SSL");
        //        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
        //                "required username=\"xxxx\" password=\"xxxx\"");
        //        properties.setProperty("sasl.mechanism","PALIN_");
        //        properties.setProperty("bootstrap.servers","127.0.0.1:9092");


        // create the Producer - 프로듀서 생성
        // Key, Valuse 모두 String Type
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java","hello world");

        // send date - Kafka로 데이터 전송
        producer.send(producerRecord);

        // flush and close the producer - 프로듀서에 전송된 내용을 반영하고 종료
        producer.flush();
        producer.close();
    }
}
