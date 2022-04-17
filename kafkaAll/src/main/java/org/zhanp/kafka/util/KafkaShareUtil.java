package org.zhanp.kafka.util;

/**
 * @author zhanpeng
 */
public class KafkaShareUtil {
    private static boolean isRunning = true;

    public static void stop() {
        isRunning = false;
    }

    public static void recover() {
        isRunning = true;
    }

    public static boolean running() {
        return isRunning;
    }
}
