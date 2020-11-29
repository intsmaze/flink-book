package com.intsmaze.flink.streaming.operator.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class IterateTemplate {

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
        env.setParallelism(1);

        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("flink", 33));
        list.add(new Tuple2<>("strom", 32));
        list.add(new Tuple2<>("spark", 15));
        list.add(new Tuple2<>("java", 18));
        list.add(new Tuple2<>("python", 31));
        list.add(new Tuple2<>("scala", 29));


        DataStream<Tuple2<String, Integer>> inputStream = env.fromCollection(list);

        IterativeStream<Tuple2<String, Integer>> itStream = inputStream
                .iterate(5000);

        SplitStream<Tuple2<String, Integer>> split = itStream
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        Thread.sleep(1000);
                        System.out.println("迭代流上面调用逻辑处理方法，参数为:" + value);
                        return new Tuple2<>(value.f0, --value.f1);
                    }
                }).split(new OutputSelector<Tuple2<String, Integer>>() {
                    @Override
                    public Iterable<String> select(Tuple2<String, Integer> value) {
                        List<String> output = new ArrayList<>();
                        if (value.f1 > 30) {
                            System.out.println("返回迭代数据:" + value);
                            output.add("iterate");
                        } else {
                            output.add("output");
                        }
                        return output;
                    }
                });

        itStream.closeWith(split.select("iterate"));

        split.select("output").print("output:");

        env.execute("IterateTemplate");

    }

}
