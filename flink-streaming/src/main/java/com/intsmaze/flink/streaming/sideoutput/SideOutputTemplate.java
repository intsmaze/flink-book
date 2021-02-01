//package com.intsmaze.flink.streaming.sideoutput;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.OutputTag;
//import org.junit.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * github地址: https://github.com/intsmaze
// * 博客地址：https://www.cnblogs.com/intsmaze/
// * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
// *
// * @auther: intsmaze(刘洋)
// * @date: 2020/10/15 18:33
// */
//public class SideOutputTemplate {
//
//
//    public static final String[] DATA = new String[]{
//            "In addition to the main stream that results from DataStream operations",
//            "When using side outputs",
//            "We recommend you",
//            "you first need to define an OutputTag that will be used to identify a side output stream"
//    };
//
//
//    final static OutputTag<String> REJECTED_WORDS_TAG = new OutputTag<String>("rejected") {
//    };
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Test
//    public void testSideOutput() throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> inputStream = env.fromElements(DATA);
//
//        SingleOutputStreamOperator<String> processStream = inputStream
//                .process(new ProcessFunction<String, String>() {
//                             @Override
//                             public void processElement(String value, Context ctx, Collector<String> out) {
//                                 if (value.length() < 20) {
//                                     ctx.output(REJECTED_WORDS_TAG, value);
//                                 } else if (value.length() >= 20) {
//                                     out.collect(value);
//                                 }
//                             }
//                         }
//                );
//
//        DataStream<String> rejectedStream = processStream.getSideOutput(REJECTED_WORDS_TAG);
//        rejectedStream.print("rejected ");
//
//        DataStream<Tuple2<String, Integer>> resultStream = processStream
//                .map(new MapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String value) {
//                        return new Tuple2<>(value, value.split(" ").length);
//                    }
//                });
//        resultStream.print("count ");
//
//        env.execute("test Side Output");
//    }
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Test
//    public void testCopyStream() throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> inputStream = env.fromElements(DATA);
//
//        DataStream<String> rejectedStream = inputStream
//                .filter((FilterFunction<String>) value -> {
//                    if (value.length() < 20) {
//                        return true;
//                    }
//                    return false;
//                });
//        rejectedStream.print("rejected");
//
//        DataStream<String> acceptStream = inputStream
//                .filter((FilterFunction<String>) value -> {
//                    if (value.length() >= 20) {
//                        return true;
//                    }
//                    return false;
//                });
//
//        DataStream<Tuple2<String, Integer>> resultStream = acceptStream
//                .map(new MapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String value) {
//                        return new Tuple2<>(value, value.split(" ").length);
//                    }
//                });
//
//        resultStream.print("count");
//        env.execute("CopyStream");
//    }
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Test
//    public void testSplitStream() throws Exception {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> inputStream = env.fromElements(DATA);
//
//        SplitStream<String> splitStream = inputStream.split(new OutputSelector<String>() {
//            @Override
//            public Iterable<String> select(String value) {
//                List<String> output = new ArrayList<String>();
//                if (value.length() >= 20) {
//                    output.add("count");
//                } else if (value.length() < 20) {
//                    output.add("rejuct");
//                }
//                return output;
//            }
//        });
//
//        DataStream<String> rejuctStream = splitStream.select("rejuct");
//        rejuctStream.print("rejuct");
//
//        DataStream<String> acceptStream = splitStream.select("count");
//        DataStream<Tuple2<String, Integer>> resultStream = acceptStream
//                .map(new MapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String value) {
//                        return new Tuple2<>(value, value.split(" ").length);
//                    }
//                });
//        resultStream.print("count");
//
//        env.execute("test Split Stream");
//    }
//
//
//}
