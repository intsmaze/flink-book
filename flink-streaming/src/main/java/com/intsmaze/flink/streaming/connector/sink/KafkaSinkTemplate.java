package com.intsmaze.flink.streaming.connector.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.junit.Test;

import java.util.*;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class KafkaSinkTemplate {


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
    public void sinkForKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> streamSource = env.fromElements("1", "2", "3");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
                "192.168.19.201:9092",
                "flink-intsmaze",
                new SimpleStringSchema());

        kafkaProducer.setLogFailuresOnly(false);
        kafkaProducer.setWriteTimestampToKafka(true);

        streamSource.addSink(kafkaProducer);

        env.execute("KafkaSink");
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
    public void sinkToKafkaSerializationSchema() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tuple2<Integer, String>> list = new ArrayList<Tuple2<Integer, String>>();
        list.add(new Tuple2(1, "intsmaze"));
        list.add(new Tuple2(5, "spark"));
        list.add(new Tuple2(6, "storm"));
        list.add(new Tuple2(33, "liuyang"));

        DataStream<Tuple2<Integer, String>> streamSource = env.fromCollection(list);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.19.201:9092");

        FlinkKafkaProducer<Tuple2<Integer, String>> flinkKafkaProducer = new FlinkKafkaProducer(
                "intsmaze-discover-1",
                new KafkaSerializationSchema(),
                properties);

        streamSource.addSink(flinkKafkaProducer);

        env.execute("KafkaSink");
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
    public void sinkToKafkaPartiton() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tuple2<Integer, String>> list = new ArrayList<Tuple2<Integer, String>>();
        list.add(new Tuple2(1, "intsmaze"));
        list.add(new Tuple2(5, "spark"));
        list.add(new Tuple2(6, "storm"));
        list.add(new Tuple2(33, "liuyang"));


        DataStream<Tuple2<Integer, String>> streamSource = env.fromCollection(list);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.19.201:9092");

        FlinkKafkaProducer<Tuple2<Integer, String>> producerPartition = new FlinkKafkaProducer(
                "intsmaze-discover-1",
                new KafkaKeyedSerializationSchema(),
                properties,
                Optional.of(new KafkaPartitioner()));

        streamSource.addSink(producerPartition);
        env.execute("KafkaSink");
    }


}
