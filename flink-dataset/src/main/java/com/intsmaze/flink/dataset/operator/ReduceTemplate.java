package com.intsmaze.flink.dataset.operator;

import com.intsmaze.flink.dataset.bean.Trade;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

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
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("123XXXXX", 899, "2018-06"));
        list.add(new Trade("123XXXXX", 699, "2018-06"));
        list.add(new Trade("188XXXXX", 88, "2018-07"));
        list.add(new Trade("188XXXXX", 69, "2018-07"));
        list.add(new Trade("158XXXXX", 100, "2018-06"));
        list.add(new Trade("158XXXXX", 1000, "2018-06"));

        DataSet<Trade> dataSource = env
                .fromCollection(list);

        DataSet<Trade> resultStream = dataSource
                .groupBy("cardNum")
                .reduce(new ReduceFunction<Trade>() {
                    /**
                     * github地址: https://github.com/intsmaze
                     * 博客地址：https://www.cnblogs.com/intsmaze/
                     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
                     *
                     * @auther: intsmaze(刘洋)
                     * @date: 2020/10/15 18:33
                     */
                    @Override
                    public Trade reduce(Trade value1, Trade value2) {
                        System.out.println(Thread.currentThread().getName() + "-----" + value1 + ":" + value2);
                        return new Trade(value1.getCardNum(), value1.getTrade() + value2.getTrade(), "----");
                    }
                });
        resultStream.print("输出结果");
        env.execute("Reduce Template");

        DataSet<Trade> fullReduce = dataSource.reduce(new ReduceFunction<Trade>() {
            /**
             * github地址: https://github.com/intsmaze
             * 博客地址：https://www.cnblogs.com/intsmaze/
             * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
             *
             * @auther: intsmaze(刘洋)
             * @date: 2020/10/15 18:33
             */
            @Override
            public Trade reduce(Trade value1, Trade value2) throws Exception {
                return new Trade(value1.getCardNum(), value1.getTrade() + value2.getTrade(), "----");
            }
        });
        fullReduce.print("full data Reduce ");

    }


}