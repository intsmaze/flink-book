package com.intsmaze.flink.dataset.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class MapPartitionTemplate {

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
        env.setParallelism(2);

        DataSet<Long> dataSet = env.generateSequence(1, 5);

        DataSet<String> result = dataSet.mapPartition(new MapPartitionFunction<Long, String>() {
            @Override
            public void mapPartition(Iterable<Long> values, Collector<String> out) {
                long count = 0;
                String result = "";
                for (Long elements : values) {
                    count++;
                    result = StringUtils.join(result, ",", elements);
                }
                out.collect("分区中迭代器内元素:" + result + " 分区中迭代器内元素的数量:" + count);
            }
        });
        result.print("输出结果");
        env.execute("MapPartition Template");
    }

}