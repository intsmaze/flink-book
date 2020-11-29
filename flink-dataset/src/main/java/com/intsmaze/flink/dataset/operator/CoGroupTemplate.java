package com.intsmaze.flink.dataset.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class CoGroupTemplate {


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        List<Tuple2<Integer, String>> commodityList = new ArrayList<Tuple2<Integer, String>>();
        List<Tuple2<String, Integer>> orderList = new ArrayList<Tuple2<String, Integer>>();

        commodityList.add(new Tuple2<>(1, "手机"));
        commodityList.add(new Tuple2<>(2, "电脑"));
        commodityList.add(new Tuple2<>(3, "移动电源"));

        orderList.add(new Tuple2<String, Integer>("张三", 1));
        orderList.add(new Tuple2<String, Integer>("赵六", 1));
        orderList.add(new Tuple2<String, Integer>("李四", 2));
        orderList.add(new Tuple2<String, Integer>("刘七", 3));
        orderList.add(new Tuple2<String, Integer>("王五", 2));

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, String>> commodityDataSet = env.fromCollection(commodityList);
        DataSet<Tuple2<String, Integer>> orderDataSet = env.fromCollection(orderList);

        DataSet<Tuple2<String, String>> result =
                commodityDataSet.coGroup(orderDataSet)
                        .where("f0")
                        .equalTo("f1")
                        .with(new CustomCoGroupFunction());

        result.print("输出结果");

        env.execute("CoGroup Template");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static class CustomCoGroupFunction implements CoGroupFunction<Tuple2<Integer, String>, Tuple2<String, Integer>, Tuple2<String, String>> {
        @Override
        public void coGroup(Iterable<Tuple2<Integer, String>> first, Iterable<Tuple2<String, Integer>> second,
                            Collector<Tuple2<String, String>> out) {
            String names = "";
            Iterator<Tuple2<String, Integer>> secondIterator = second.iterator();
            while (secondIterator.hasNext()) {
                Tuple2<String, Integer> next = secondIterator.next();
                names = StringUtils.join(names, " ", next.f0);
            }
            Iterator<Tuple2<Integer, String>> firstIterator = first.iterator();
            while (firstIterator.hasNext()) {
                Tuple2<Integer, String> next = firstIterator.next();
                out.collect(new Tuple2<>(next.f1, names));
            }
        }
    }
}