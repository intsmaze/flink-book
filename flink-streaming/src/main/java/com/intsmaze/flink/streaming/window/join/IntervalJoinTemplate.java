package com.intsmaze.flink.streaming.window.join;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class IntervalJoinTemplate {

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<ClickBean> clicksData = PrepareData.getClicksData();
        DataStream<ClickBean> clickStream = env.fromCollection(clicksData)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ClickBean>() {
                    @Override
                    public long extractTimestamp(ClickBean element, long previousElementTimestamp) {
                        return element.getVisitTime().getTime();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(ClickBean lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.getVisitTime().getTime() - 1);
                    }
                });

        List<Trade> personData = PrepareData.getTradeData();
        DataStream<Trade> tradeStream = env.fromCollection(personData)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Trade>() {
                    @Override
                    public long extractTimestamp(Trade element, long previousElementTimestamp) {
                        return element.getTradeTime().getTime();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(Trade lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.getTradeTime().getTime() - 1);
                    }
                });


        KeyedStream<ClickBean, String> clickKeyedStream = clickStream
                .keyBy((KeySelector<ClickBean, String>) value -> value.getUser());
        KeyedStream<Trade, String> tradeKeyedStream = tradeStream
                .keyBy((KeySelector<Trade, String>) value -> value.getName());

        DataStream<String> process = clickKeyedStream.intervalJoin(tradeKeyedStream)
                .between(Time.minutes(-30), Time.minutes(20))
                .process(new ProcessJoinFunction<ClickBean, Trade, String>() {
                    @Override
                    public void processElement(ClickBean left, Trade right,

                                               Context ctx, Collector<String> out) {
                        out.collect(left.toString() + " : " + right.toString());
                    }
                });

        process.print();
        env.execute("WindowIntervalJoinTumblingTemplate");
    }

}
