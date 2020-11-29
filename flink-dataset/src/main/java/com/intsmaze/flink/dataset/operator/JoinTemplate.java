package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.junit.Before;
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
public class JoinTemplate {

    List<Tuple2<Integer, String>> commodityList = new ArrayList<Tuple2<Integer, String>>();
    List<Tuple2<String, Integer>> orderList = new ArrayList<Tuple2<String, Integer>>();

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Before
    public void before() {
        commodityList.add(new Tuple2<>(1, "手机"));
        commodityList.add(new Tuple2<>(2, "电脑"));
        commodityList.add(new Tuple2<>(3, "移动电源"));

        orderList.add(new Tuple2<String, Integer>("张三", 1));
        orderList.add(new Tuple2<String, Integer>("赵六", 1));
        orderList.add(new Tuple2<String, Integer>("李四", 2));
        orderList.add(new Tuple2<String, Integer>("刘七", 3));
        orderList.add(new Tuple2<String, Integer>("王五", 2));
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
    public void testDefaultJoin() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSet<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                commodityDataSet.join(orderDataSet)
                        .where("f0")
                        .equalTo("f1");

        result.print("输出结果");

        env.execute("Join Template");
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
    public void testJoinWithJoinFunction() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSet<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);

        DataSet<Tuple2<String, String>> result =
                commodityDataSet.join(orderDataSet)
                        .where("f0")
                        .equalTo("f1")
                        // applying the JoinFunction on joining pairs
                        .with(new CustomJoinFunction());

        result.print("输出结果");

        env.execute("JoinWithJoinFunction Template");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public class CustomJoinFunction implements JoinFunction<Tuple2<Integer, String>,
            Tuple2<String, Integer>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> join(Tuple2<Integer, String> first,
                                           Tuple2<String, Integer> second) {
            return new Tuple2<>(first.f1, second.f0);
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
    @Test
    public void testJoinWithFlatJoinFunction() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSet<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);

        DataSet<Tuple2<String, String>> result =
                commodityDataSet.join(orderDataSet)
                        .where("f0")
                        .equalTo("f1")
                        .with(new CustomFlatJoinFunction());

        result.print("输出结果");

        env.execute("JoinWithFlatJoinFunction Template");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public class CustomFlatJoinFunction implements FlatJoinFunction<Tuple2<Integer, String>,
            Tuple2<String, Integer>, Tuple2<String, String>> {
        @Override
        public void join(Tuple2<Integer, String> first, Tuple2<String, Integer> second,
                         Collector<Tuple2<String, String>> out) throws Exception {
            out.collect(new Tuple2<>(first.f1 + "", second.f0));
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
    @Test
    public void testJoinProjection() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSet<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);


        DataSet<Tuple2<String, String>> result =
                commodityDataSet.joinWithHuge(orderDataSet)
                        .where("f0")
                        .equalTo("f1")
                        .projectFirst(1)
                        .projectSecond();

        result.print("输出结果");

        env.execute("JoinProjection Template");
    }

}