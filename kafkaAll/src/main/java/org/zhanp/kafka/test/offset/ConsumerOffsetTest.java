package org.zhanp.kafka.test.offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.zhanp.kafka.constant.KafkaConstants;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者偏移测试
 */
@Slf4j
public class ConsumerOffsetTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.Broker.LOCAL_BROKER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER.O2_TEST_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            TopicPartition topicPartition = new TopicPartition(KafkaConstants.Topic.O2_TEST_TOPIC, 0);
            consumer.assign(Collections.singleton(topicPartition));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1100);
                if (consumerRecords.isEmpty()) {
//                    log.info("consumerRecords is empty");
                    continue;
                }
                List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                log.info("last consume offset is: [{}]", records.get(records.size() - 1).offset());
                consumer.commitSync();
                Map<TopicPartition, OffsetAndMetadata> offsetByTp = consumer.committed(Collections.singleton(topicPartition));
                OffsetAndMetadata offsetAndMetadata = offsetByTp.get(topicPartition);
                log.info("commit offset is: [{}]", offsetAndMetadata.offset());
                log.info("the offset of next record: [{}]", consumer.position(topicPartition));
            }
        }
    }
}
