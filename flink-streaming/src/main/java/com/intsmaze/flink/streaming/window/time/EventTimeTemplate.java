package com.intsmaze.flink.streaming.window.time;

import com.intsmaze.flink.streaming.window.source.Data;
import com.intsmaze.flink.streaming.window.source.SourceWithTimestamps;
import com.intsmaze.flink.streaming.window.source.SourceWithTimestampsWatermarks;
import com.intsmaze.flink.streaming.window.time.bean.EventBean;
import com.intsmaze.flink.streaming.window.time.watermark.EventTimePunctuatedWaterMarks;
import com.intsmaze.flink.streaming.window.time.watermark.EventTimeWaterMarks;
import com.intsmaze.flink.streaming.window.util.TimeUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Calendar;
import java.util.Date;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class EventTimeTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        while (true) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            long offset = calendar.get(Calendar.SECOND);
            Thread.sleep(100);
            System.out.println(offset);
            if (offset <= 0) {
                break;
            } else if (offset >= 30 && offset <= 35) {
                break;
            }
        }

        System.out.println("start time" + TimeUtils.getHHmmss(new Date()));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        ExecutionConfig config = env.getConfig();
        config.setAutoWatermarkInterval(5000);

        DataStream<EventBean> stream = env.addSource(new SourceWithTimestamps(10000))
                .assignTimestampsAndWatermarks(new EventTimeWaterMarks());
//                .assignTimestampsAndWatermarks(new EventTimePunctuatedWaterMarks());

//        DataStream<EventBean> stream = env.addSource(new SourceWithTimestampsWatermarks(10000));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(Data.date);
        System.out.println("send time:" + TimeUtils.getHHmmss(Data.date));

        String flag = "输出延迟元素";
        if ("不允许延迟".equals(flag)) {
            DataStream<EventBean> result = stream
                    .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                    .reduce(new ReduceFunction<EventBean>() {
                                @Override
                                public EventBean reduce(EventBean value1, EventBean value2) {
                                    value1.getList().add(value2.getList().get(0));
                                    return value1;
                                }
                            }
                    );
            result.print("输出结果");
        } else if ("允许延迟".equals(flag)) {
            DataStream<EventBean> result = stream
                    .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                    .allowedLateness(Time.seconds(15))
                    .reduce((value1, value2) ->
                            {
                                value1.getList().add(value2.getList().get(0));
                                return value1;
                            }
                    );
            result.print("result **********************************");
        } else if ("输出延迟元素".equals(flag)) {
            final OutputTag<EventBean> lateOutputTag = new OutputTag<EventBean>("late-data") {
            };

            SingleOutputStreamOperator<EventBean> result = stream
                    .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
                    .allowedLateness(Time.seconds(15))
                    .sideOutputLateData(lateOutputTag)
                    .reduce((value1, value2) ->
                            {
                                value1.getList().add(value2.getList().get(0));
                                return value1;
                            }
                    );

            DataStream<EventBean> lateStream = result.getSideOutput(lateOutputTag);
            lateStream.print("late elements is  --->>>");
            result.print("输出结果");
        }
        env.execute("EventTimeTemplate");
    }

}