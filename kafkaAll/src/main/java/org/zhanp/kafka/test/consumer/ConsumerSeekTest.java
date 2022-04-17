package org.zhanp.kafka.test.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.zhanp.kafka.constant.KafkaConstants;
import org.zhanp.kafka.util.KafkaShareUtil;
import org.zhanp.kafka.util.PropertiesHelper;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * 消费者偏移seek测试
 *
 * @author zhanpeng.jiang@hand-china.com
 */
@Slf4j
public class ConsumerSeekTest {
    public static void main(String[] args) {
        Properties properties = PropertiesHelper.consumerProperties();
        // 关闭自动提交
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singleton(KafkaConstants.Topic.O2_TEST_TOPIC));
            if (KafkaShareUtil.running()) {
                Set<TopicPartition> assignment = new HashSet<>();
                while (assignment.isEmpty()) {
                    kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                    // 更新assignment
                    assignment = kafkaConsumer.assignment();
                }
                // 已经通过poll拿到assignment，可以进行seek操作
                kafkaConsumer.seekToBeginning(assignment);
                for (TopicPartition tp : assignment) {
                    log.info("next fetch offset is [{}], topic:[{}], partition:[{}]", kafkaConsumer.position(tp), tp.topic(), tp.partition());
                }
            }
            // 提交位移，观察offset. 没问题
            kafkaConsumer.commitSync();
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            log.info("commit position:[{}]", kafkaConsumer.committed(assignment));
        }
    }
}
