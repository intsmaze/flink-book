package com.intsmaze.flink.streaming.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class KafkaSourceSinkTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties conProperties = new Properties();
        conProperties.setProperty("bootstrap.servers", "192.168.19.201:9092");
        conProperties.setProperty("group.id", "intsmaze");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "intsmaze-pojo", new SimpleStringSchema(), conProperties);

        DataStream<String> streamSource = env.addSource(kafkaConsumer);

        DataStream<String> mapStream = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                int number = Integer.valueOf(value) + 1;
                Thread.sleep(100);
                System.out.println("number：" + number);
                return String.valueOf(number);
            }
        });


        Properties proProperties = new Properties();
        proProperties.put("bootstrap.servers", "192.168.19.201:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer(
                "intsmaze-pojo", new SimpleStringSchema(), proProperties);

        mapStream.addSink(kafkaProducer);

        DataStream<String> testSource = env.fromElements("1");

        testSource.addSink(kafkaProducer);

        env.execute("KafkaSourceSinkTemplate");
    }
}
