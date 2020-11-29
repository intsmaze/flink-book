package com.intsmaze.flink.streaming.connector.source;

import com.google.gson.Gson;
import com.intsmaze.flink.streaming.bean.SchemaBean;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class KafkaSourceTemplate {


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void sourceForKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.19.201:9092");
        properties.setProperty("group.id", "intsmaze");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flink-intsmaze", new SimpleStringSchema(), properties);


        kafkaConsumer.setStartFromGroupOffsets();
//		kafkaConsumer.setStartFromEarliest();
        kafkaConsumer.setStartFromLatest();
//		kafkaConsumer.setStartFromTimestamp(System.currentTimeMillis()-5000);
        DataStream<String> streamSource = env.addSource(kafkaConsumer);
        streamSource.print("kafka data is:");

        env.execute("sourceForKafka");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void sourceSpecificOffsetsKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.19.201:9092");
        properties.setProperty("group.id", "intsmaze");

        String topic = "flink-intsmaze-two";
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), properties);
        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();

        specificStartOffsets.put(new KafkaTopicPartition(topic, 0), 2L);
        specificStartOffsets.put(new KafkaTopicPartition(topic, 1), 3L);
        kafkaConsumer.setStartFromSpecificOffsets(specificStartOffsets);

        DataStream<String> streamSource = env.addSource(kafkaConsumer);
        streamSource.print("kafka data is:");

        env.execute("sourceSpecificOffsetsKafka");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void kafkaSourceDiscover() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.19.201:9092");
        properties.setProperty("group.id", "intsmaze");

        properties.put("flink.partition-discovery.interval-millis", "100");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                java.util.regex.Pattern.compile("intsmaze-discover-[0-9]"),
                new SimpleStringSchema(), properties);


    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Deprecated
    @Test
    public void kafkaSourceDeserialization() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.19.201:9092");
        properties.setProperty("group.id", "intsmaze");

        FlinkKafkaConsumer<SchemaBean> myConsumer = new FlinkKafkaConsumer<>(
                "intsmaze-pojo", new KafkaDeserializationSchema(), properties);

        FlinkKafkaConsumer<Tuple2<String, SchemaBean>> myConsumer1 = new FlinkKafkaConsumer<>(
                "intsmaze-pojo", new KafkaKeyedDeserializationSchema(), properties);

        env.addSource(myConsumer).print();

        env.execute("KafkaSourceAndSink");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void kafkaSourceTimestampsAndWatermarks() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.19.201:9092");
        properties.setProperty("group.id", "intsmaze");

        FlinkKafkaConsumer<KafkaMess> kafkaConsumer = new FlinkKafkaConsumer<KafkaMess>(
                "intsmaze-pojo", new AbstractDeserializationSchema<KafkaMess>() {
            @Override
            public KafkaMess deserialize(byte[] message) throws IOException {
                Gson gson = new Gson();
                return gson.fromJson(new String(message), KafkaMess.class);
            }
        }, properties);

        kafkaConsumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<KafkaMess>() {
            @Override
            public long extractTimestamp(KafkaMess element, long previousElementTimestamp) {
                return element.getTime();
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });

        DataStream<KafkaMess> streamSource = env.addSource(kafkaConsumer);
        streamSource.print();

        env.execute("kafkaSourceTimestampsAndWatermarks");
    }
}
