package com.intsmaze.flink.streaming.partition;

import com.intsmaze.flink.streaming.bean.Trade;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class RebalanceTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        final String flag = "rebalance分区策略前子任务名称:";

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("185XXX", 899, "2018"));
        list.add(new Trade("155XXX", 1111, "2019"));
        list.add(new Trade("155XXX", 1199, "2019"));
        list.add(new Trade("185XXX", 899, "2018"));
        list.add(new Trade("138XXX", 19, "2019"));
        list.add(new Trade("138XXX", 399, "2020"));
        list.add(new Trade("138XXX", 399, "2020"));
        list.add(new Trade("138XXX", 399, "2020"));

        DataStream<Trade> inputStream = env.fromCollection(list);

        DataStream<Trade> mapOne = inputStream.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                if (key.indexOf("185") >= 0) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }, "cardNum");

        DataStream<Trade> mapTwo = mapOne.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                RuntimeContext context = getRuntimeContext();
                String subtaskName = context.getTaskNameWithSubtasks();
                int subtaskIndexOf = context.getIndexOfThisSubtask();
                System.out.println("元素值:" + value + flag + subtaskName
                        + " ,子任务编号:" + subtaskIndexOf);
                return value;
            }
        }).setParallelism(2);

        DataStream<Trade> mapThree = mapTwo.rebalance();

        DataStream<Trade> mapfour = mapThree.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                RuntimeContext context = getRuntimeContext();
                String subtaskName = context.getTaskNameWithSubtasks();
                int subtaskIndexOf = context.getIndexOfThisSubtask();
                System.out.println("元素值:" + value + " 分区策略后子任务名:" + subtaskName + " ,子任务编号:" + subtaskIndexOf);
                return value;
            }
        });
        mapfour.print();
        env.execute("Physical partitioning");

    }
}
