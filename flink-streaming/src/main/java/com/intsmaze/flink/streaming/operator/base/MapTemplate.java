package com.intsmaze.flink.streaming.operator.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class MapTemplate {

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

        DataStream<Long> streamSource = env.generateSequence(1, 5);

        DataStream<Tuple2<Long, Integer>> mapStream = streamSource
                .map(new MapFunction<Long, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(Long values) {
                        return new Tuple2<>(values * 100, values.hashCode());
                    }
                });
        mapStream.print("输出结果");
        env.execute("MapTemplate");
    }

}