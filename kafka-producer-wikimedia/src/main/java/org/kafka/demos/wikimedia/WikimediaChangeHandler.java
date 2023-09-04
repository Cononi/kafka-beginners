package org.kafka.demos.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);
    private KafkaProducer<String,String> kafkaProducer;
    private String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // Stream이 열렸을 때.
    }

    @Override
    public void onClosed(){
        // 스트림이 닫혔을 때
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        // Http Stream에서 온 메세지를 스트림이 수신했다는 뜻이다.
        // asynchronous - 비동기 코드 작성
        /**
         * 토픽명, messageEvent로 전달되는 데이터 문자열을 전달
         */
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        // Stream 읽기 중단, 프로듀서 닫기, 종료 처리시 사용
        log.error("Error in Stream Reading", t);
    }
}
