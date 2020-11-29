package com.intsmaze.flink.streaming.state.key;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class MapStateFlatMap extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {


    public static Logger LOG = LoggerFactory.getLogger(MapStateFlatMap.class);

    public transient MapState<Integer, Integer> mapState;

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
        MapStateDescriptor<Integer, Integer> descriptor = new MapStateDescriptor("MapStateFlatMap", Long.class, Long.class);
        mapState = getRuntimeContext().getMapState(descriptor);
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
        mapState.put(input.f0, input.f1);
        Iterator<Map.Entry<Integer, Integer>> iterator = mapState.iterator();
        LOG.info("..........................");
        while (iterator.hasNext()) {
            Map.Entry<Integer, Integer> map = iterator.next();
            LOG.info("{},{} ... {}", Thread.currentThread().getName(), map.getKey(), map.getValue());
        }
        out.collect(input);
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

        keyedStream.flatMap(new MapStateFlatMap()).print();

        env.execute("Intsmaze MapStateFlatMap");
    }

}
