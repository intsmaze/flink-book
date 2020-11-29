package com.intsmaze.flink.table.sqlapi;

import com.intsmaze.flink.table.PrepareData;
import com.intsmaze.flink.table.bean.ClickBean;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import javax.annotation.Nullable;
import java.sql.Timestamp;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class OverWindowTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testDataStream() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<ClickBean> dataStream = env.fromCollection(PrepareData.getClicksData());

        dataStream = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickBean>() {
            /**
             * github地址: https://github.com/intsmaze
             * 博客地址：https://www.cnblogs.com/intsmaze/
             * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
             *
             * @auther: intsmaze(刘洋)
             * @date: 2020/10/15 18:33
             */
            @Override
            public long extractTimestamp(ClickBean element, long previousElementTimestamp) {
                Timestamp timestamp = element.getTime();
                return timestamp.getTime();
            }

            /**
             * github地址: https://github.com/intsmaze
             * 博客地址：https://www.cnblogs.com/intsmaze/
             * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
             *
             * @auther: intsmaze(刘洋)
             * @date: 2020/10/15 18:33
             */
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });


        tableEnv.registerDataStream("Clicks", dataStream, "user,time,url,VisitTime.rowtime");

        String sqlQuery = "SELECT user,COUNT(url) OVER ( " +
                "  PARTITION BY user " +
                "  ORDER BY VisitTime " +
                "  ROWS BETWEEN UNBOUNDED  PRECEDING AND CURRENT ROW) " +
                "FROM Clicks ";

        tableEnv.toRetractStream(tableEnv.sqlQuery(sqlQuery), Row.class).print();

        env.execute();

    }

}
