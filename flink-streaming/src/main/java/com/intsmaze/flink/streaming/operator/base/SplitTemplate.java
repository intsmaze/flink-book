package com.intsmaze.flink.streaming.operator.base;

import com.intsmaze.flink.streaming.bean.Trade;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
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
public class SplitTemplate {

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

        List<Trade> list = new ArrayList<Trade>();
        list.add(new Trade("185XXX", 899, "周一"));
        list.add(new Trade("155XXX", 1199, "周二"));
        list.add(new Trade("138XXX", 19, "周三"));
        DataStream<Trade> dataStream = env.fromCollection(list);

        SplitStream splitStream = dataStream.split(new OutputSelector<Trade>() {
            @Override
            public Iterable<String> select(Trade value) {
                List<String> output = new ArrayList<String>();
                if (value.getTrade() < 100) {
                    output.add("Small amount");
                    output.add("Small amount backup");
                } else if (value.getTrade() > 100) {
                    output.add("Large amount");
                }
                return output;
            }
        });

        splitStream.select("Small amount")
                .print("Small amount:");

        splitStream.select("Large amount").
                print("Large amount:");

        splitStream.select("Small amount backup", "Large amount")
                .print("Small amount backup and Large amount");

        env.execute("SplitTemplate");
    }
}