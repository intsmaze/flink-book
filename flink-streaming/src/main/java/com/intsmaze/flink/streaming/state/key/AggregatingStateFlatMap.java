package com.intsmaze.flink.streaming.state.key;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
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
public class AggregatingStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    public static Logger LOG = LoggerFactory.getLogger(AggregatingStateFlatMap.class);

    public transient AggregatingState<Tuple2<Integer, Integer>, String> aggregatingState;


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
        AggregatingStateDescriptor descriptor = new AggregatingStateDescriptor("AggregatingState",
                new AggregateFunction<Tuple2<Integer, Integer>, AverageAccumulator, Double>() {

                    /**
                     * github地址: https://github.com/intsmaze
                     * 博客地址：https://www.cnblogs.com/intsmaze/
                     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
                     *
                     * @auther: intsmaze(刘洋)
                     * @date: 2020/10/15 18:33
                     */
                    @Override
                    public AverageAccumulator createAccumulator() {
                        return new AverageAccumulator();
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
                    public AverageAccumulator add(Tuple2<Integer, Integer> value, AverageAccumulator accumulator) {
                        accumulator.add(value.f1);
                        return accumulator;
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
                    public Double getResult(AverageAccumulator accumulator) {
                        return accumulator.getLocalValue();
                    }

                    @Override
                    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
                        a.add(b.getCount(), b.getSum());
                        return a;
                    }
                },
                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                }));

        aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
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
        aggregatingState.add(input);
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

        keyedStream.flatMap(new AggregatingStateFlatMap()).print();

        env.execute("Intsmaze AggregatingStateFlatMap");
    }


}
