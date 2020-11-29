package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class FlatMapTemplate {

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

        DataSet<Tuple2<String, Integer>> dataSet = env.fromElements(
                new Tuple2<>("liu yang", 1),
                new Tuple2<>("my blog is intsmaze", 2),
                new Tuple2<>("hello flink", 2));

        DataSet<Tuple1<String>> flatMapDataSet = dataSet.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple1<String>>() {
            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple1<String>> out) {

                if ("liu yang".equals(value.f0)) {
                    return;
                } else {
                    out.collect(new Tuple1<String>("Not included intsmaze：" + value.f0));
                }
            }
        });
        flatMapDataSet.print("输出结果");

        env.execute("FlatMapTemplate");

    }
}