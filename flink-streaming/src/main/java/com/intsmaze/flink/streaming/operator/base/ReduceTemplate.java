package com.intsmaze.flink.streaming.operator.base;

import com.intsmaze.flink.streaming.bean.Trade;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class ReduceTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testKeyByReduceBean() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("123XXXXX", 899, "2018-06"));
        list.add(new Trade("123XXXXX", 699, "2018-06"));
        list.add(new Trade("188XXXXX", 88, "2018-07"));
        list.add(new Trade("188XXXXX", 69, "2018-07"));
        list.add(new Trade("158XXXXX", 100, "2018-06"));
        list.add(new Trade("158XXXXX", 1000, "2018-06"));

        DataStream<Trade> dataSource = env
                .fromCollection(list);

        DataStream<Trade> resultStream = dataSource
                .keyBy("cardNum")
                .reduce(new ReduceFunction<Trade>() {
                    @Override
                    public Trade reduce(Trade value1, Trade value2) {
                        String theadName = Thread.currentThread().getName();
                        String info = theadName + "-----" + value1 + ":" + value2;
                        System.out.println(info);
                        return new Trade(value1.getCardNum(), value1.getTrade() + value2.getTrade(), "----");
                    }
                });
        resultStream.print("输出结果");
        env.execute("Reduce Template");
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
    public void testKeyByReduceNestedTuple() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<Tuple2<Integer, String>, Integer, Integer>> list = new ArrayList<Tuple3<Tuple2<Integer, String>, Integer, Integer>>();
        list.add(new Tuple3(new Tuple2(1, "intsmaze"), 1, 11));
        list.add(new Tuple3(new Tuple2(1, "intsmaze"), 1, 22));
        list.add(new Tuple3(new Tuple2(33, "liuyang"), 33, 333));

        DataStream<Tuple3<Tuple2<Integer, String>, Integer, Integer>> dataStream = env
                .fromCollection(list)
                .keyBy("f0.f1")
                .reduce(new ReduceFunction<Tuple3<Tuple2<Integer, String>, Integer, Integer>>() {
                    @Override
                    public Tuple3<Tuple2<Integer, String>, Integer, Integer> reduce(
                            Tuple3<Tuple2<Integer, String>, Integer, Integer> value1,
                            Tuple3<Tuple2<Integer, String>, Integer, Integer> value2) {
                        Tuple2 tuple2 = value1.getField(0);
                        int v1 = value1.getField(1);
                        int v2 = value2.getField(1);
                        int v3 = value1.getField(2);
                        int v4 = value2.getField(2);
                        return new Tuple3(tuple2, v1 + v2, v3 + v4);
                    }
                });
        dataStream.print();
        env.execute("KeyByTemplate");
    }


}