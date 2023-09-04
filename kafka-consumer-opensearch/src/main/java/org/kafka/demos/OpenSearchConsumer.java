package org.kafka.demos;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OpenSearchConsumer {

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        String indexName = "wikimedia";

        // OpenSearch Client Create
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Kafka Client Creat
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // OpenSearch Query and index create
        try(openSearchClient; consumer) {
            // 존재 여부 확인
            boolean indexExists = openSearchClient
                    .indices()
                    .exists(new GetIndexRequest(indexName)
                            , RequestOptions.DEFAULT);
            if(!indexExists){
                // 쿼리 인덱스 생성
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                // 쿼리 실행
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                // 인덱스 생성 확인 및 메세지
                log.info("{} 인덱스가 생성되었습니다.!",indexName);
            } else {
                log.info("{} 인덱스가 이미 존재합니다.!",indexName);
            }

            // consumer 구독
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while (true) {
                // 데이터가 없을 경우에 코드 블록의 해당 줄을 3초간 차단한다.
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                // 대량 요청 처리
                BulkRequest bulkRequests = new BulkRequest();

                for(ConsumerRecord<String,String> record: records){

                    // storyegy 1
                    // ( 레코드 좌표)를 사용하여 ID를 정의
//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        // storyegy 2
                        // JSON 값에서 ID를 추출하도록 하자.
                        String id = extractId(record.value());

                        // Consumer 데이터를 IndexRequest로 변환하고 JSON형태로 만들어서 OpenSearch로 전송
                        // ID를 제공함으로써 중복 데이터가 없다. - 기존에
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        // response Index 응답값 받기
//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        // OpenSearch에 삽입하는 대신 bulkrequest.add를 넣고 indexRequest 전달하기
                        bulkRequests.add(indexRequest);

                        // ID로 응답하기 때문에 ID 반환값 받기
//                        log.info(response.getId());
                    } catch (Exception e){

                    }

                }

                // 대량 요청에 있는 작업 수가 0보다 큰 실제 수일 때만 수행
                if(bulkRequests.numberOfActions() > 0){
                    // 대량 전달 처리
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequests, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");
                    // 짧은 지연을 더해 대량 작업을 사용할 확률을 높인다.
                    try {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }


                    // commit offsets after the batch is consumed
                    // 오프셋이 동시에 커밋되도록 설정
                    consumer.commitSync();
                    log.info("Offsets hava been committed");
                }

            }
        }
    }

    private static String extractId(String json){
        // GSON으로 JSON 데이터 추출
        return JsonParser.parseString(json)
                .getAsJsonObject()// json데이터 오브젝트 가져오기
                .get("meta") // 오브젝트 내에 meta 가져오기
                .getAsJsonObject() // meta object 가져오기
                .get("id") // id 데이터 가져오기
                .getAsString(); // string으로 변환
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        // create Producer Properties - 프로듀서 설정 생성
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092"); // 브로커와 연결


        // create consumer configs - 역직렬화
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 최신만 읽어들여 히스토리가 많이 남지 않게 한다.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    public static RestHighLevelClient createOpenSearchClient() {
        // 연결할 주소
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
}
