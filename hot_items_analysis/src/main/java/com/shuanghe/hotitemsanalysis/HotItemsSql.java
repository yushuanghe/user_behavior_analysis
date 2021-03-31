package com.shuanghe.hotitemsanalysis;

import com.shuanghe.hotitemsanalysis.constant.KafkaConstant;
import com.shuanghe.hotitemsanalysis.map.Data6ParserMapFunc;
import com.shuanghe.hotitemsanalysis.model.RawData6Event;
import com.shuanghe.hotitemsanalysis.util.ConfigurationManager;
import com.shuanghe.hotitemsanalysis.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @author yushu
 */
public class HotItemsSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ParameterTool kafkaConfig = ConfigurationManager.getFlinkConfig(KafkaConstant.KAFKA_PROP_NAME);

        //从kafka读数据
        String kafkaGroupId = kafkaConfig.get(KafkaConstant.KAFKA_GROUP_ID);
        String kafkaTopic = kafkaConfig.get(KafkaConstant.KAFKA_TOPIC_NAME);
        Properties prop = KafkaUtils.getKafkaProp(kafkaConfig, kafkaGroupId);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(), prop);
        KafkaUtils.setKafkaOffset(kafkaConsumer, kafkaConfig);

        //从文件中读取数据
        DataStream<RawData6Event> inputStream = env.addSource(kafkaConsumer)
                .flatMap(new Data6ParserMapFunc())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<RawData6Event>(Time.milliseconds(1)) {
                    @Override
                    public long extractTimestamp(RawData6Event element) {
                        return element.getTimestamp();
                    }
                });
        //inputStream.print();

        //定义表执行环境
        EnvironmentSettings settings = EnvironmentSettings.
                newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //基于 DataStream 创建table
        Table dataTable = tableEnv.fromDataStream(inputStream, $("appId"), $("behavior"), $("timestamp").rowtime().as("ts"));
        tableEnv.createTemporaryView("dataTable", dataTable);

        //table api 进行开创聚合统计
        Table aggTable = dataTable
                .filter($("behavior").isEqual("start"))
                .window(Slide.over(lit(1).hours())
                        .every(lit(1).seconds())
                        .on($("ts"))
                        .as("w"))
                .groupBy($("appId"), $("w"))
                .select($("appId")
                        , $("w").end().as("windowEnd")
                        , $("appId").count().as("cnt")
                );
        tableEnv.createTemporaryView("aggTable", aggTable);
        //tableEnv.toRetractStream(aggTable, Row.class).print();

        //用sql实现topn
        // TODO: 2021-03-29 table api 混用 sql 有bug 
        //Table resultTable = tableEnv.sqlQuery("select * from (select *,row_number() over (partition by windowEnd " +
        //        "order by cnt desc) as rnk from aggTable) where rnk<=5");
        ////Table resultTable = tableEnv.sqlQuery("select * from aggTable");

        //纯sql实现
        Table resultTable = tableEnv.sqlQuery("select * from (select *,row_number() over (partition by windowEnd order by cnt desc) as rnk from (select appId,hop_end(ts,interval '1' second,interval '1' hour) as windowEnd,count(1) as cnt from dataTable where behavior='start' group by appId,hop(ts,interval '1' second,interval '1' hour)) t) t where rnk<=5");
        tableEnv.toRetractStream(resultTable, Row.class).print();

        env.execute("hot_items_sql");
    }
}
