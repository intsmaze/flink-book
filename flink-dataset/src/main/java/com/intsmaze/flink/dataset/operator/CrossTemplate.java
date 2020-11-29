package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class CrossTemplate {

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
    public void before() throws Exception {
        commodityList.add(new Tuple2<Integer, String>(1, "手机"));
        orderList.add(new Tuple2<String, Integer>("张三", 1));
        orderList.add(new Tuple2<String, Integer>("赵六", 2));
        orderList.add(new Tuple2<String, Integer>("李四", 3));
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
    public void testDefaultCross() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSource<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                commodityDataSet.crossWithHuge(orderDataSet);

        result.print("输出结果");

        env.execute("Cross Template");
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
    public void testWithCrossFunction() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSource<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);

        DataSet<Tuple2<String, String>> result = commodityDataSet
                .cross(orderDataSet)
                .with(new CustomCrossFunction());

        result.print("输出结果");

        env.execute("CrossFunction Template");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public class CustomCrossFunction implements CrossFunction<Tuple2<Integer, String>, Tuple2<String, Integer>, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> cross(Tuple2<Integer, String> first,
                                            Tuple2<String, Integer> second) {
            return new Tuple2<String, String>(first.f1, second.f0);
        }
    }

}