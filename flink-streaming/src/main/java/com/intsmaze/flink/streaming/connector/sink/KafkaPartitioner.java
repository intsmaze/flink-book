package com.intsmaze.flink.streaming.connector.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class KafkaPartitioner extends FlinkKafkaPartitioner<Tuple2<Integer, String>> {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public int partition(Tuple2<Integer, String> record, byte[] key, byte[] value, String targetTopic, int[] partitions) {

        System.out.println("数据流中的元素:" + record.toString());
        System.out.println("消息的key值:" + new String(key));
        System.out.println("消息的主体内容：" + new String(value));
        System.out.println("targetTopic:" + targetTopic);

        String sKey = new String(key);
        Integer integer = Integer.valueOf(sKey);
        if (integer < 100) {
            return partitions.length - 1;
        } else {
            return partitions.length - 2;
        }
    }
}
