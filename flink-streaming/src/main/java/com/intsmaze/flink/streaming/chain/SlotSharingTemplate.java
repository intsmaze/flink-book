package com.intsmaze.flink.streaming.chain;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class SlotSharingTemplate {

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
        env.setParallelism(3);
        DataStream<Tuple2<String, Integer>> inputStream = env.addSource(new ChainSource());

        DataStream<Tuple2<String, Integer>> filter = inputStream.filter(new RichFilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) {
                System.out.println("filter操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return true;
            }
        });

        DataStream<Tuple2<String, Integer>> mapOne = filter.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                System.out.println("map-one操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return value;
            }
        });

        mapOne = ((SingleOutputStreamOperator<Tuple2<String, Integer>>) mapOne).slotSharingGroup("custom-name");

        DataStream<Tuple2<String, Integer>> mapTwo = mapOne.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                System.out.println("map-two操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return value;
            }
        });
        mapTwo.print();

        env.execute("slotSharingGroup");
    }

}
