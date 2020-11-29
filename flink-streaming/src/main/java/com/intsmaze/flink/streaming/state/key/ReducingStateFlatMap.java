package com.intsmaze.flink.streaming.state.key;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class ReducingStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    public static Logger LOG = LoggerFactory.getLogger(ReducingStateFlatMap.class);

    public transient ReducingState<Tuple2<Integer, Integer>> reducingState;

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public void open(Configuration config) {
        LOG.info("{},{}", Thread.currentThread().getName(), "恢复或初始化状态");
        ReducingStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                new ReducingStateDescriptor<Tuple2<Integer, Integer>>(
                        "ReducingStateFlatMap",
                        new ReduceFunction<Tuple2<Integer, Integer>>() {
                            @Override
                            public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) {
                                LOG.info("ReduceState reduceFunction   " + value1.f1 + ".........." + value2.f1);
                                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                            }
                        },
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        }));
        reducingState = getRuntimeContext().getReducingState(descriptor);
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
        reducingState.add(input);
        out.collect(reducingState.get());
    }

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

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = KeyStateBase.before(env);

        keyedStream.flatMap(new ReducingStateFlatMap()).print("输出结果");

        env.execute("Intsmaze ReducingStateFlatMap");
    }

}
