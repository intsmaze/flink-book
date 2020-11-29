package com.intsmaze.flink.streaming.window.process;


import com.intsmaze.flink.streaming.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
@Deprecated
public class ProcessWindowFoldTemplate {
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

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));


        DataStream<Tuple3<String, Integer, String>> fold = streamSource.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .fold(Tuple3.of("Start", 0, "time"), new FoldFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>>() {

                    @Override
                    public Tuple3<String, Integer, String> fold(Tuple3<String, Integer, String> accumulator, Tuple3<String, Integer, String> value) throws Exception {

                        if (value.f1 > accumulator.f1) {
                            accumulator.f1 = value.f1;
                            accumulator.f0 = accumulator.f0 + "--" + value.f0;
                        }
                        return accumulator;
                    }
                });


        fold.print();

        env.execute("TumblingWindows");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static class UserDefinedProcessWindow
            extends ProcessWindowFunction<Tuple3<String, Integer, String>, Tuple3<String, Integer, String>, String, TimeWindow> {


        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, Integer, String>> elements, Collector<Tuple3<String, Integer, String>> out) throws Exception {

        }
    }
}
