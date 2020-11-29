package com.intsmaze.flink.streaming.partition;

import com.intsmaze.flink.streaming.bean.Trade;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class CustomTemplate {

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

        final String flag = " 分区策略前子任务名称:";
        DataStream<Trade> inputStream = env.addSource(new PartitionSource());

        DataStream<Trade> mapOne = inputStream.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                RuntimeContext context = getRuntimeContext();
                String subtaskName = context.getTaskNameWithSubtasks();
                int subtaskIndexOf = context.getIndexOfThisSubtask();
                System.out.println("元素值:" + value + flag + subtaskName
                        + " ,子任务编号:" + subtaskIndexOf);
                return value;
            }
        });

//        DataStream<Trade> mapTwo = mapOne.partitionCustom(new MyPartitioner(), "cardNum");
        DataStream<Trade> mapTwo = mapOne.partitionCustom(new MyTradePartitioner(), new KeySelector<Trade, Trade>() {
            @Override
            public Trade getKey(Trade trade) {
                return trade;
            }
        });

        DataStream<Trade> mapThree = mapTwo.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                RuntimeContext context = getRuntimeContext();
                String subtaskName = context.getTaskNameWithSubtasks();
                int subtaskIndexOf = context.getIndexOfThisSubtask();
                System.out.println("元素值:" + value + " 分区策略后子任务名称:" + subtaskName
                        + " ,子任务编号:" + subtaskIndexOf);
                return value;
            }
        });
        mapThree.print();
        env.execute("Physical partitioning");
    }

}