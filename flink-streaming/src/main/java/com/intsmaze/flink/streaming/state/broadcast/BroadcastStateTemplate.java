package com.intsmaze.flink.streaming.state.broadcast;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class BroadcastStateTemplate {

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
        env.setParallelism(2);

        DataStream<Tuple2<Integer, String>> ruleStream = env.addSource(new RuleSource());

        DataStream<Date> mainStream = env.addSource(new CustomSource(1000L));

        final MapStateDescriptor<Integer, String> stateDesc = new MapStateDescriptor<>(
                "broadcast-state", Integer.class, String.class
        );

        BroadcastStream<Tuple2<Integer, String>> broadcastStream = ruleStream.broadcast(stateDesc);

        DataStream<Tuple2<String, String>> result = mainStream.connect(broadcastStream)
                .process(new CustomBroadcastProcessFunction());

        result.print("输出结果");

        env.execute("BroadcastState Template");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    private static class CustomBroadcastProcessFunction extends
            BroadcastProcessFunction<Date, Tuple2<Integer, String>, Tuple2<String, String>> {

        private transient MapStateDescriptor<Integer, String> descriptor;

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void open(Configuration parameters) {
            descriptor = new MapStateDescriptor<>(
                    "broadcast-state", Integer.class, String.class
            );
        }

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void processElement(Date value, ReadOnlyContext ctx,
                                   Collector<Tuple2<String, String>> out) throws Exception {
            String formatRule = "";
            for (Map.Entry<Integer, String> entry :
                    ctx.getBroadcastState(descriptor).immutableEntries()) {
                if (entry.getKey() == 1) {
                    formatRule = entry.getValue();
                }
            }
            if (StringUtils.isNotBlank(formatRule)) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String originalDate = format.format(value);
                format = new SimpleDateFormat(formatRule);
                String formatDate = format.format(value);
                out.collect(Tuple2.of("主数据流元素:" + originalDate, "应用规则后的格式：" + formatDate));
            }
        }

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void processBroadcastElement(Tuple2<Integer, String> value, Context ctx,
                                            Collector<Tuple2<String, String>> out) throws Exception {
            BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(descriptor);
            broadcastState.put(value.f0, value.f1);
            out.collect(new Tuple2<>("广播状态中新增元素", value.toString()));
        }

    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static class CustomSource implements SourceFunction<Date> {

        private Long sleep;

        public CustomSource(Long sleep) {
            this.sleep = sleep;
        }

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void run(SourceContext<Date> ctx) throws InterruptedException {
            while (true) {
                ctx.collect(new Date());
                Thread.sleep(sleep);
            }
        }

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void cancel() {
        }

    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static class RuleSource implements SourceFunction<Tuple2<Integer, String>> {

        private String[] format = new String[]{"yyyy-MM-dd HH:mm", "yyyy-MM-dd HH",
                "yyyy-MM-dd", "yyyy-MM", "yyyy"};

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            while (true) {
                for (int i = 0; i < format.length; i++) {
                    String rule = format[i];
                    ctx.collect(new Tuple2<>(1, rule));
                    Thread.sleep(5000);
                }
            }
        }

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void cancel() {

        }

    }

}

