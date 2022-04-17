package org.zhanp.kafka.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.zhanp.kafka.constant.KafkaConstants;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class ProducerTest {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.Broker.LOCAL_BROKER);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        final String testValue = "test_value:" + UUID.randomUUID().toString().substring(0, 5);
        String testTopic = KafkaConstants.Topic.O2_TEST_TOPIC;
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(testTopic, 0, null, testValue);
        producer.send(producerRecord);
//        Future<RecordMetadata> send = producer.send(producerRecord);
//        while (!send.isDone()) {
//        }
        // producer有缓冲区，所以需要刷一下流
        producer.close();
        log.info("Producer produce topic[{}], partition[{}], value[{}]", testTopic, 0, testValue);
    }
}
