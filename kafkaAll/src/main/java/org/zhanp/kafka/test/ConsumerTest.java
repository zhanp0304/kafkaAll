package org.zhanp.kafka.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.zhanp.kafka.constant.KafkaConstants;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerTest {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.Broker.LOCAL_BROKER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER.O2_TEST_CONSUMER_GROUP);
        /*
            新增一个consumer group，如果发现此group是新的，在broker上没有任何信息和offset，
            则根据auto.offset.reset去设置该group的初始offset,后续再有consumer加入此group，就会从上一次的group offset继续往后消费。

            所以要区分场景使用：
            1. 想要消费某个topic已有的信息，设置auto.offset.reset=earliest, 新建group,启动该group下的consumer，
               就会把offset设置为topic的start位置，就会开始消费已有消息。

            2. 不想消费已有信息（可能是以前积压了太多），我现在只想验证新的数据和代码。 则需要设置auto.offset.reset=latest(默认),
               新建group, 启动该group下的consumer,就会把offset设置为topic的end+1位置。后面就开始消费新的消息。
         */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Collections.singleton(KafkaConstants.Topic.O2_TEST_TOPIC));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(Long.MAX_VALUE, ChronoUnit.MILLIS));
            Iterable<ConsumerRecord<String, String>> records = consumerRecords.records(KafkaConstants.Topic.O2_TEST_TOPIC);
            records.forEach(consumerRecord -> {
                String topic = consumerRecord.topic();
                int partition = consumerRecord.partition();
                String value = consumerRecord.value();
                log.info("Consumer topic[{}], partition[{}], value[{}]", topic, partition, value);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
