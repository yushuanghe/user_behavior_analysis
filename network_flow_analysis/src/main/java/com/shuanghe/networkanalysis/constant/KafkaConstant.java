package com.shuanghe.networkanalysis.constant;

/**
 * Description:
 * Date: 2021-02-26
 * Time: 17:03
 *
 * @author yushu
 */
public class KafkaConstant {
    public static final String KAFKA_PROP_NAME = "network_flow_analysis/src/main/resources/kafka_config.properties";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";

    public static final String KAFKA_TOPIC_NAME = "kafka.topic";

    public static final String KAFKA_AUTO_OFFSET_RESET = "auto.offset.reset";

    public static final String KAFKA_START_TIMESTAMP = "kafka.start.from.timestamp";

    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String KAFKA_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String KAFKA_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
}
