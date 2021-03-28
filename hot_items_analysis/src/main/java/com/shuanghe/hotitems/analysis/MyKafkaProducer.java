package com.shuanghe.hotitems.analysis;

import com.shuanghe.hotitems.analysis.constant.KafkaConstant;
import com.shuanghe.hotitems.analysis.util.ConfigurationManager;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author yushu
 */
public class MyKafkaProducer {
    public static void main(String[] args) throws IOException {
        ParameterTool kafkaConfig = ConfigurationManager.getFlinkConfig(KafkaConstant.KAFKA_PROP_NAME);
        Properties prop = new Properties();
        prop.setProperty(KafkaConstant.KAFKA_BOOTSTRAP_SERVERS, kafkaConfig.get(KafkaConstant.KAFKA_BOOTSTRAP_SERVERS));
        prop.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer kafkaProducer = new KafkaProducer<String, String>(prop);
        //从文件读取数据，写入kafka
        BufferedReader in =
                new BufferedReader(new InputStreamReader(new FileInputStream("hot_items_analysis/src/main/resources" +
                        "/data-6.log")));
        String line = in.readLine();
        while (line != null) {
            ProducerRecord recode = new ProducerRecord<String, String>(kafkaConfig.get(KafkaConstant.KAFKA_TOPIC_NAME), line);
            kafkaProducer.send(recode);
            line = in.readLine();
        }

        kafkaProducer.close();
    }
}
