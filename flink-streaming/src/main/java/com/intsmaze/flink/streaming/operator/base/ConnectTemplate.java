package com.intsmaze.flink.streaming.operator.base;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

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
public class ConnectTemplate {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static ConnectedStreams init() {
        List<Long> listLong = new ArrayList<Long>();
        listLong.add(1L);
        listLong.add(2L);

        List<String> listStr = new ArrayList<String>();
        listStr.add("www cnblogs com intsmaze");
        listStr.add("hello intsmaze");
        listStr.add("hello flink");
        listStr.add("hello java");

        DataStream<Long> longStream = env.fromCollection(listLong);
        DataStream<String> strStream = env.fromCollection(listStr);
        return longStream.connect(strStream);
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testConnectMap() throws Exception {

        ConnectedStreams<Long, String> connectedStreams = init();

        DataStream<String> connectedMap = connectedStreams
                .map(new CoMapFunction<Long, String, String>() {
                    @Override
                    public String map1(Long value) {
                        return "数据来自元素类型为long的流" + value;
                    }

                    @Override
                    public String map2(String value) {
                        return "数据来自元素类型为String的流" + value;
                    }
                });
        connectedMap.print();
        env.execute("CoMapFunction");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testConnectFlatMap() throws Exception {

        ConnectedStreams<Long, String> connectedStreams = init();

        DataStream<String> connectedFlatMap = connectedStreams
                .flatMap(new CoFlatMapFunction<Long, String, String>() {
                    @Override
                    public void flatMap1(Long value, Collector<String> out) {
                        out.collect(value.toString());
                    }

                    @Override
                    public void flatMap2(String value, Collector<String> out) {
                        for (String word : value.split(" ")) {
                            out.collect(word);
                        }
                    }
                });
        connectedFlatMap.print();
        env.execute("CoFlatMapFunction");
    }
}
