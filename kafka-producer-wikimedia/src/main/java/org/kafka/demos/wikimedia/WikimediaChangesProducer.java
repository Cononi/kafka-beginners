package org.kafka.demos.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.kafka.demos.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class);
    public static void main(String[] args) throws InterruptedException {

        // create Producer Properties
        Properties properties = Config.kafkaProducerProperties();

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        log.info(properties.toString());
        /**
         * OKhttp 방식
         */
        EventHandler eventHandler = new WikimediaChangeHandler(producer,topic);
        // 데이터를 받는 출처 링크
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        // 이벤트 소스에 데이터 넣기
        EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(url));
        EventSource eventSource = builder.build();

        // start the Producer in another - 다른 스레드에서 프로듀서 실행
        eventSource.start();

        // 10분동안 프로듀싱 하고 그동안 프로그램을 중지시키도록 설정
        // Thread.sleep()은 java 1.5 이전에만 사용하고 현재는 TimeUnit-Sleep()을 사용하고있다.
        // 주요 목적은 현재 실행중인 프로그램의 실행을 잠시 멈추고 싶을때 사용한다.
        TimeUnit.MINUTES.sleep(10);
    }
}
