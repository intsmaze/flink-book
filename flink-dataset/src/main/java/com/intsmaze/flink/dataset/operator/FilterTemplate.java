package com.intsmaze.flink.dataset.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class FilterTemplate {


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
        DataSet<Long> dataSet = env.generateSequence(1, 5);

        DataSet<Long> filterDataSet = dataSet.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) {
                if (value == 2L || value == 4L) {
                    return false;
                }
                return true;
            }
        });
        filterDataSet.print("输出结果");
        env.execute("Filter Template");
    }
}