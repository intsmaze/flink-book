package com.intsmaze.flink.streaming.window.process.trigger;

import com.intsmaze.flink.streaming.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
public class TumblingWindowTriggerTemplate {

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

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<Tuple2<String, List<Integer>>> map = streamSource.map(new MapFunction<Tuple3<String, Integer, String>, Tuple2<String, List<Integer>>>() {

            @Override
            public Tuple2<String, List<Integer>> map(Tuple3<String, Integer, String> value) {
                List list = new ArrayList();
                list.add(value.f1);
                return Tuple2.of(value.f0, list);
            }
        });

        DataStream<Tuple2<String, List<Integer>>> reduce = map.keyBy("f0")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .trigger(CountTriggerDebug.of(2))
                .reduce((new ReduceFunction<Tuple2<String, List<Integer>>>() {
                    @Override
                    public Tuple2<String, List<Integer>> reduce(Tuple2<String, List<Integer>> value1, Tuple2<String, List<Integer>> value2) {
                        value1.f1.add(value2.f1.get(0));
                        return value1;
                    }
                }));

        reduce.print();

        env.execute("TumblingWindowTriggerTemplate");
    }


}
