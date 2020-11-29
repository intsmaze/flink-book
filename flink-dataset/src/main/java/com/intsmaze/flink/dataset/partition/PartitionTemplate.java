package com.intsmaze.flink.dataset.partition;

import com.intsmaze.flink.dataset.bean.Trade;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
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
public class PartitionTemplate {

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
        env.setParallelism(3);

        final String flag = " 分区策略前子任务名称:";

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("185XXX", 1199, "2018"));
        list.add(new Trade("155XXX", 1111, "2019"));
        list.add(new Trade("155XXX", 20, "2019"));
        list.add(new Trade("185XXX", 2899, "2018"));
        list.add(new Trade("138XXX", 19, "2019"));
        list.add(new Trade("138XXX", 399, "2020"));
        list.add(new Trade("138XXX", 39, "2020"));
        list.add(new Trade("138XXX", 99, "2020"));

        DataSet<Trade> dataSource = env.fromCollection(list);

        DataSet<Trade> mapResult = dataSource.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                String taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println("元素值:" + value + flag + taskNameWithSubtasks + " ,子任务编号:" + indexOfThisSubtask);
                return value;
            }
        }).setParallelism(3);

        mapResult = mapResult.rebalance();
//        mapResult = mapResult.partitionByHash("cardNum");
//        mapResult = mapResult.partitionByRange("cardNum");
//        mapResult = mapResult.sortPartition("cardNum", Order.ASCENDING);


        mapResult = mapResult.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                System.out.println("元素值:" + value + " 分区策略后子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks()
                        + " ,子任务编号:" + getRuntimeContext().getIndexOfThisSubtask());
                return value;
            }
        });
        mapResult.print("输出结果");

        env.execute("partitioning");
    }

}