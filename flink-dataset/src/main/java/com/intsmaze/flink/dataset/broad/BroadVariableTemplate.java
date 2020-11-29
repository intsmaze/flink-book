package com.intsmaze.flink.dataset.broad;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class BroadVariableTemplate {


    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> broadcastDataSet = env.fromElements(1, 2, 3);

        DataSet<String> data = env.fromElements("a", "b");

        DataSet<String> result = data.map(new BroadMapTemplate())
                .withBroadcastSet(broadcastDataSet, "Broadcast Variable Name");

        result.print("输出结果");

        env.execute("BroadVariable Template");
    }

    /**
     * @author ：intsmaze
     * @date ：Created in 2020/3/6 20:23
     * @description： https://www.cnblogs.com/intsmaze/
     * @modified By：
     */
    public static class BroadMapTemplate extends RichMapFunction<String, String> {

        private List<Integer> broadcastCollection;

        @Override
        public void open(Configuration parameters) {
            broadcastCollection = getRuntimeContext()
                    .getBroadcastVariable("Broadcast Variable Name");
        }

        @Override
        public String map(String value) {
            return value + broadcastCollection.toString();
        }
    }
}
