package com.intsmaze.flink.table.sqlapi.groupwindows;

import com.intsmaze.flink.table.PrepareData;
import com.intsmaze.flink.table.bean.ClickBean;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class GroupWindowTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testProcessTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        String[] names = new String[]{"id", "number"};
        TypeInformation[] types = new TypeInformation[]{Types.INT(), Types.INT()};
        DataStream<Row> inputStream = env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext ctx) throws Exception {
                int counter = 1;
                while (true) {
                    Row row = Row.of(counter % 2, counter);
                    ctx.collect(row);
                    System.out.println("send data :" + row);
                    Thread.sleep(3000);
                    counter++;
                }
            }

            @Override
            public void cancel() {
            }
        }).returns(Types.ROW(names, types));

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.registerDataStream("test", inputStream, "id,number,autoAddTime.proctime");

        String sqlQuery = "SELECT id," +
                "sum(number) " +
                ",TUMBLE_START(autoAddTime, INTERVAL '14' SECOND) AS wStart" +
                ", TUMBLE_END(autoAddTime, INTERVAL '14' SECOND) AS wEnd " +
                ", TUMBLE_PROCTIME(autoAddTime, INTERVAL '14' SECOND) AS wEnd " +
                "FROM test " +
                "GROUP BY TUMBLE(autoAddTime, INTERVAL '14' SECOND), id ";

        Table result = tableEnv.sqlQuery(sqlQuery);

        tableEnv.toAppendStream(result, Row.class).print("toAppendStream");
        env.execute();
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        List<ClickBean> clicksData = PrepareData.getClicksData();
        Collections.shuffle(clicksData);
        DataStream<ClickBean> dataStream = env.fromCollection(clicksData);

        //提取时间戳并分配水印
        dataStream = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickBean>() {
            @Override
            public long extractTimestamp(ClickBean element, long previousElementTimestamp) {
                Timestamp timestamp = element.getTime();
                return timestamp.getTime();
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });

        tableEnv.registerDataStream("Clicks", dataStream, "id,user,VisitTime.rowtime,url");

//        tableEnv.registerDataStream("Clicks", dataStream, "id,user,time,url,VisitTime.rowtime");

        String sqlQuery = "SELECT user AS name," +
                "count(url) " +
                ",TUMBLE_START(VisitTime, INTERVAL '1' HOUR) " +
                ",TUMBLE_ROWTIME(VisitTime, INTERVAL '1' HOUR) " +
                ",TUMBLE_END(VisitTime, INTERVAL '1' HOUR)  " +
                "FROM Clicks " +
                "GROUP BY TUMBLE(VisitTime, INTERVAL '1' HOUR), user ";

        Table table = tableEnv.sqlQuery(sqlQuery);
        DataStream<Row> resultStream = tableEnv.toAppendStream(table, Row.class);

        resultStream.print();

        env.execute();
    }

}
