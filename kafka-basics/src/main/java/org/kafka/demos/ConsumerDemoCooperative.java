package org.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        String groupId = "my-java-application";
        String topic = "demo_java";

        log.info("I am a Kafka Consumer!");

        // create Producer Properties - 프로듀서 설정 생성
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092"); // 브로커와 연결


        // create consumer configs - 역직렬화
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        // 점진적, 협력적 리밸런싱 설정하기,
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        // create a consumer - 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to ther main thread - 메인 스레드, 즉 현재 스레드의 참조를 얻는다.
        final Thread mainThread = Thread.currentThread();

        // adding the Shutdown Hook
        // 새로운 쓰레드를 생성해서 로직 실행.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // 종료를 감지함, cunsumer.wakeup 호출 후 종료하겠다.
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                // 메인 스레드에 합류후 메인 스레드의 코드 실행을 허용
                try {
                    // 특정한 쓰레드가 종료될 때 까지 기다린다. (여기서는 현재 이 쓰레드로 지정된 상태)
                    mainThread.join();
                    log.info("쓰레드가 모두 종료되었습니다.");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        // 어느 지점이 되면 consumer.poll이 wakeup 예외를 던지게된다.
        try {
            // Subscribe to a Topic - 토픽 구독
            consumer.subscribe(Arrays.asList(topic));

            // poll for data - 토픽으로 부터 데이터 받아오기
            while (true) {
                // 1000 밀리초동안 대기
                ConsumerRecords<String, String> recodes =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : recodes) {
                    log.info("key: " + record.key() + ", value: " + record.value());
                    log.info("partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            // 컨슈머가 종료를 시작
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            // 컨슈머에 예기치 못한 예외 발생
            log.error("Unexpected exception in ther consumer", e);
        } finally {
            // close the consumer, this will also commit offsets
            consumer.close();
            // 컨슈머가 우아하게 종료
            log.info("The consumer is now gracefully shut down");
        }
    }
}
