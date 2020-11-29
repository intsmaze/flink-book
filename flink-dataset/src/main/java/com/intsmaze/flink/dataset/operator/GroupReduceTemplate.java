package com.intsmaze.flink.dataset.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.operators.Order;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class GroupReduceTemplate {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple2<String, Integer>> dataSet;

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
        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
        list.add(new Tuple2<>("张三", 15));
        list.add(new Tuple2<>("张三", 20));
        list.add(new Tuple2<>("张三", 21));
        list.add(new Tuple2<>("李四", 31));
        list.add(new Tuple2<>("李四", 38));
        list.add(new Tuple2<>("李四", 45));
        list.add(new Tuple2<>("王五", 55));
        list.add(new Tuple2<>("王五", 67));

        dataSet = env.fromCollection(list);
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @After
    public void after() throws Exception {
        env.execute();
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
    public void reduceGroup() {
        DataSet<Tuple2<Integer, String>> result = dataSet.groupBy("f0")
                .reduceGroup(new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<Integer, String>>() {
                    /**
                     * github地址: https://github.com/intsmaze
                     * 博客地址：https://www.cnblogs.com/intsmaze/
                     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
                     *
                     * @auther: intsmaze(刘洋)
                     * @date: 2020/10/15 18:33
                     */
                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<Integer, String>> out) {
                        Set<String> uniqString = new HashSet<String>();
                        Integer key = null;

                        Iterator<Tuple2<String, Integer>> iterator = values.iterator();
                        while (iterator.hasNext()) {
                            Tuple2<String, Integer> next = iterator.next();
                            key = next.f1;
                            uniqString.add(next.f0);
                        }

                        for (String s : uniqString) {
                            out.collect(new Tuple2<Integer, String>(key, s));
                        }
                    }
                });
        result.print("输出结果");
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
    public void testSortedGroups() throws Exception {
        dataSet.groupBy("f0")
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new CustomSortedGroups())
                .print("排序后的处理结果 ");

        DataSet<Tuple2<String, String>> result = dataSet
                .groupBy("f0")
                .reduceGroup(new CustomSortedGroups());
        result.print("未排序的处理结果");

    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public class CustomSortedGroups implements GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, String>> {
        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, String>> out) {
            List<Integer> list = new ArrayList<Integer>();
            String key = null;

            Iterator<Tuple2<String, Integer>> iterator = values.iterator();
            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                key = next.f0;
                list.add(next.f1);
            }
            out.collect(new Tuple2<String, String>(key, list.toString()));
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
    public void testCombinableGroups() throws Exception {
        DataSet<Tuple2<String, Integer>> result = dataSet.groupBy("f0")
                .reduceGroup(new CustomCombinableGroupReducer());

        result.print("输出结果");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public class CustomCombinableGroupReducer implements
            GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>,
            GroupCombineFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> values,
                           Collector<Tuple2<String, Integer>> out) {
            dealCompute(values, out, "调用reduce方法进行最终计算");
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
        public void combine(Iterable<Tuple2<String, Integer>> values,
                            Collector<Tuple2<String, Integer>> out) {
            dealCompute(values, out, "调用combine方法进行预计算");
        }

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        private void dealCompute(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out, String flag) {
            String key = "";
            int value = 0;
            String str = "";
            Iterator<Tuple2<String, Integer>> iterator = values.iterator();
            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                str = StringUtils.join(str, next, " ");
                key = next.f0;
                value = value + next.f1;
            }
            str = StringUtils.join(flag, str);
            System.out.println(str);
            out.collect(new Tuple2<>(key, value));
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
    public void testGroupCombine() throws Exception {
        DataSet<Tuple2<String, Integer>> result = dataSet.groupBy("f0")
                .combineGroup(new CustomCombinableGroupReducer())
                .groupBy("f0")
                .reduceGroup(new CustomCombinableGroupReducer());

        result.print("输出结果");
    }
}