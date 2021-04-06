package com.shuanghe.orderoaydetect.util;

import com.shuanghe.orderoaydetect.constant.KafkaConstant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Description:
 * Date: 2021-02-24
 * Time: 20:32
 *
 * @author yushu
 */
public class KafkaUtils {
    /**
     * 从kafka读取数据
     *
     * @param env        flink env
     * @param kafkaProps kafka配置
     * @param topic      kafka topic
     * @return
     */
    public static DataStream<String> readTextFromKafka(StreamExecutionEnvironment env, Properties kafkaProps, String topic) {
        return env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), kafkaProps));
    }

    public static Properties getKafkaProp(ParameterTool kafkaConfig, String kafkaGroupId) {
        //从kafka读数据
        Properties prop = new Properties();
        prop.setProperty(KafkaConstant.KAFKA_BOOTSTRAP_SERVERS, kafkaConfig.get(KafkaConstant.KAFKA_BOOTSTRAP_SERVERS));
        prop.setProperty("group.id", kafkaGroupId);

        prop.setProperty("key.deserializer", KafkaConstant.KAFKA_KEY_DESERIALIZER);
        prop.setProperty("value.deserializer", KafkaConstant.KAFKA_VALUE_DESERIALIZER);
        prop.setProperty(KafkaConstant.KAFKA_AUTO_OFFSET_RESET, kafkaConfig.get(KafkaConstant.KAFKA_AUTO_OFFSET_RESET));
        prop.setProperty("flink.partition-discovery.interval-millis", "30000");

        return prop;
    }

    public static void setKafkaOffset(FlinkKafkaConsumer<String> kafkaConsumer, ParameterTool kafkaConfig) {
        long kafkaStartTimestamp = kafkaConfig.getLong(KafkaConstant.KAFKA_START_TIMESTAMP);
        if (kafkaStartTimestamp == 0) {
            kafkaConsumer.setStartFromLatest();
        } else if (kafkaStartTimestamp == -1) {
            kafkaConsumer.setStartFromEarliest();
        } else {
            kafkaConsumer.setStartFromTimestamp(kafkaStartTimestamp);
        }
    }
}
