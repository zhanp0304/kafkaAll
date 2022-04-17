package org.zhanp.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.zhanp.kafka.constant.KafkaConstants;

import java.util.Properties;

/**
 * @author zhanpeng
 */
public class PropertiesHelper {

    public static Properties consumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.Broker.LOCAL_BROKER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER.O2_TEST_CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
    }

    public static Properties producerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.Broker.LOCAL_BROKER);
        return properties;
    }
}
