package org.zhanp.kafka.constant;

/**
 * kafka常量
 */
public class KafkaConstants {

    public static class Broker {
        private Broker() {

        }

        /**
         * o2_dev_broker
         */
        public static final String O2_DEV_BROKER = "172.23.16.184:9092";

        /**
         * local broker
         */
        public static final String LOCAL_BROKER = "127.0.0.1:9092";
    }


    /**
     * topic
     */
    public static class Topic {
        private Topic() {

        }

        public static String TEST_TOPIC = "testTopic";
    }


    /**
     * consumer
     */
    public static class CONSUMER {
        private CONSUMER() {

        }

        public static String TEST_CONSUMER_GROUP = "TEST_CONSUMER_GROUP_2";
    }

}
