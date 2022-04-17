package org.zhanp.kafka.test.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.zhanp.kafka.constant.KafkaConstants;
import org.zhanp.kafka.util.KafkaShareUtil;
import org.zhanp.kafka.util.PropertiesHelper;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * 可根据时间找到分区位移
 *
 * @author zhanpeng
 */
@Slf4j
public class consumerOffsetTimeTest {
    public static void main(String[] args) {
        Properties properties = PropertiesHelper.consumerProperties();
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singleton(KafkaConstants.Topic.O2_TEST_TOPIC));
            if (KafkaShareUtil.running()) {
                Set<TopicPartition> assignments = new HashSet<>();
                while (assignments.isEmpty()) {
                    kafkaConsumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
                    assignments = kafkaConsumer.assignment();
                }

                HashMap<TopicPartition, Long> timestampToSearch = new HashMap<>();
                long timestamp = System.currentTimeMillis() - (long) 30 * 60 * 1000;
                for (TopicPartition tp : assignments) {
                    // 寻找一天前的消息
                    //                timestampToSearch.put(tp, System.currentTimeMillis() - (long) 24 * 3600 * 1000);
                    // 30分钟前
                    timestampToSearch.put(tp, timestamp);
                }

                Map<TopicPartition, OffsetAndTimestamp> offsetTimestampByTp = kafkaConsumer.offsetsForTimes(timestampToSearch);

                for (TopicPartition tp : assignments) {
                    OffsetAndTimestamp offsetAndTimestamp = offsetTimestampByTp.get(tp);
                    if (offsetAndTimestamp == null) {
                        throw new IllegalStateException("no offset in timestamp:" + timestamp);
                    }
                    long offset = offsetAndTimestamp.offset();
                    Date date = new Date(offsetAndTimestamp.timestamp());
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String format = simpleDateFormat.format(date);
                    log.info("seek offset to a day before, dateTime:[{}], offset:[{}]", format, offset);

                    kafkaConsumer.seek(tp, offset);
                }

                // 消费10分钟前的消息
                while (KafkaShareUtil.running()) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(1000L, ChronoUnit.MILLIS));
                    if (consumerRecords.isEmpty()) {
                        return;
                    }
                    for (TopicPartition tp : assignments) {
                        List<ConsumerRecord<String, String>> tpRecords = consumerRecords.records(tp);
                        for (ConsumerRecord<String, String> consumerRecord : tpRecords) {
                            log.info("Consume record:[{}], partition:[{}]", consumerRecord.value(), consumerRecord.partition());
                        }
                        HashMap<TopicPartition, OffsetAndMetadata> tpOffsetMap = new HashMap<>(1);
                        tpOffsetMap.put(tp, new OffsetAndMetadata(tpRecords.get(tpRecords.size() - 1).offset()));
                        kafkaConsumer.commitSync(tpOffsetMap);
                    }
                }
            }
        }
    }
}
