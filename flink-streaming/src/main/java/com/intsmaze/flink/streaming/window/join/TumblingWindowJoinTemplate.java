package com.intsmaze.flink.streaming.window.join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class TumblingWindowJoinTemplate {

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

        List<Trade> tradeData = PrepareData.getTradeData();
        DataStream<Trade> tradeStream = env.fromCollection(tradeData)
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


        JoinedStreams.WithWindow<ClickBean, Trade, String, TimeWindow> window =
                clickStream.join(tradeStream)
                        .where((KeySelector<ClickBean, String>) value -> value.getUser())
                        .equalTo((KeySelector<Trade, String>) value -> value.getName())
                        .window(TumblingEventTimeWindows.of(Time.hours(1)));


        DataStream<String> applyJoinStream = window.apply(new JoinFunction<ClickBean, Trade, String>() {
            @Override
            public String join(ClickBean first, Trade second) {
                return first.toString() + " : " + second.toString();
            }
        });

        applyJoinStream.print("JoinFunction:");

        DataStream<String> applyFlatJoinStream = window.apply(new FlatJoinFunction<ClickBean, Trade, String>() {
            @Override
            public void join(ClickBean first, Trade second, Collector<String> out) {
                out.collect(first.getUser() + " : " + first.getVisitTime() + " : " + second.getTradeTime());
                out.collect(first.getUser() + " : " + first.getUrl() + " : " + second.getClient());
            }
        });
        applyFlatJoinStream.print("FlatJoinFunction:");

        env.execute("WindowJoinTumblingTemplate");
    }


}
