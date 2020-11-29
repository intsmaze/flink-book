package com.intsmaze.flink.streaming.state.key;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
public class TTLStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

    public static Logger LOG = LoggerFactory.getLogger(TTLStateFlatMap.class);

    public transient ValueState<Tuple2<Integer, Integer>> valueState;

    private boolean isRead;

    private boolean isReturn;

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public TTLStateFlatMap(boolean isRead, boolean isReturn) {
        this.isRead = isRead;
        this.isReturn = isReturn;
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
    public void open(Configuration config) {
        LOG.info("{},{}", Thread.currentThread().getName(), "恢复或初始化状态");
        ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor = new ValueStateDescriptor<>(
                "ValueStateTTL",
                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                }));
        StateTtlConfig.Builder builder = StateTtlConfig.newBuilder(Time.seconds(6));
        if (isRead) {
            builder = builder.setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite);
        } else {
            builder = builder.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite);
        }
        if (isReturn) {
            builder = builder.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp);
        } else {
            builder = builder.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired);
        }
        StateTtlConfig ttlConfig = builder.build();
        descriptor.enableTimeToLive(ttlConfig);
        valueState = getRuntimeContext().getState(descriptor);
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
        if (input.f1 > 10 && input.f1 < 20) {
            if (isRead) {
                LOG.info("不执行操作:{}", input);
                out.collect(input);
            } else {
                Tuple2<Integer, Integer> currentSum = valueState.value();
                LOG.info("不执行操作 状态值:{} 输入值:{}", currentSum, input);
            }
        } else {
            Tuple2<Integer, Integer> currentSum = valueState.value();
            LOG.info("currentSum before:" + currentSum);
            System.out.println("currentSum before:" + currentSum);
            if (currentSum == null) {
                currentSum = input;
            } else {
                currentSum.f1 = currentSum.f1 + input.f1;
            }
            valueState.update(currentSum);
            LOG.info("currentSum after:" + currentSum);
            out.collect(currentSum);
        }
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

        keyedStream.flatMap(new TTLStateFlatMap(true, false)).print("输出结果:");

        env.execute("Intsmaze TTLStateFlatMap");
    }
}

