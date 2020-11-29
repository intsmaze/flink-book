package com.intsmaze.flink.streaming.window.process;

import com.intsmaze.flink.streaming.window.source.SourceForWindow;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class ProcessWindowReduceTemplate {
    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(new SourceForWindow(1000));

        DataStream<Tuple2<Long, Tuple3<String, Integer, String>>> reduceStream = streamSource
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> r1, Tuple3<String, Integer, String> r2) {
                        return r1.f1 > r2.f1 ? r2 : r1;
                    }
                }, new UserDefinedProcessWindow());
        reduceStream.print();

        env.execute("ProcessWindowReduceTemplate");
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
            extends ProcessWindowFunction<Tuple3<String, Integer, String>, Tuple2<Long, Tuple3<String, Integer, String>>, String, TimeWindow> {
        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void process(String key,
                            Context context,
                            Iterable<Tuple3<String, Integer, String>> minReadings,
                            Collector<Tuple2<Long, Tuple3<String, Integer, String>>> out) {

            Iterator<Tuple3<String, Integer, String>> iterator = minReadings.iterator();
            if (iterator.hasNext()) {
                Tuple3<String, Integer, String> min = iterator.next();
                Long start = context.window().getStart();
                out.collect(new Tuple2<Long, Tuple3<String, Integer, String>>(start, min));
            }
        }
    }
}


