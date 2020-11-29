package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

import static org.apache.flink.api.java.aggregation.Aggregations.MIN;
import static org.apache.flink.api.java.aggregation.Aggregations.SUM;


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
public class AggregationsTemplate {


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<String, Integer, Double>> list = new ArrayList<>();
        list.add(new Tuple3<>("张三", 15, 999.9));
        list.add(new Tuple3<>("张三", 30, 1899.0));
        list.add(new Tuple3<>("张三", 21, 3000.89));
        list.add(new Tuple3<>("李四", 31, 188.88));
        list.add(new Tuple3<>("王五", 55, 99.99));
        list.add(new Tuple3<>("王五", 67, 18.88));

        DataSource<Tuple3<String, Integer, Double>> dataSource = env.fromCollection(list);

        dataSource.groupBy("f0").sum(0)
                .aggregate(SUM, 1)
                .print("aggregate sum");

        dataSource.groupBy("f0")
                .aggregate(SUM, 1)
                .and(MIN, 2)
                .print("aggregate sum and min");

        dataSource.groupBy("f0")
                .sum(1).print("sum");

        dataSource.groupBy("f0")
                .max(1)
                .print("max");


        dataSource
                .groupBy("f0")
                .minBy(1)
                .print("minBy");

        dataSource
                .groupBy("f0")
                .maxBy(1)
                .print("maxBy");

        env.execute();
    }
}