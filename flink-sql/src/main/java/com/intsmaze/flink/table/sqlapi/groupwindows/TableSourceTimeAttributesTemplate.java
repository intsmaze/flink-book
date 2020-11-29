package com.intsmaze.flink.table.sqlapi.groupwindows;

import com.intsmaze.flink.table.util.TimeStampUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
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
public class TableSourceTimeAttributesTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static List<Row> getRowDataSetTimeAttributes() throws ParseException {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(10, "intsmaze", TimeStampUtils.stringToTime("2019-07-28 12:00:00"), "www.cnblogs.com/intsmaze/"));
        data.add(Row.of(8, "flink", TimeStampUtils.stringToTime("2019-07-28 12:00:00"), "www.cnblogs.com/intsmaze/"));
        data.add(Row.of(3, "intsmaze", TimeStampUtils.stringToTime("2019-07-28 12:02:00"), "www.cnblogs.com/intsmaze/"));
        data.add(Row.of(1, "intsmaze", TimeStampUtils.stringToTime("2019-07-28 14:00:00"), "github.com/intsmaze"));
        data.add(Row.of(7, "flink", TimeStampUtils.stringToTime("2019-07-28 14:30:00"), "www.cnblogs.com/intsmaze/"));
        data.add(Row.of(9, "flink", TimeStampUtils.stringToTime("2019-07-28 14:40:00"), "www.cnblogs.com/intsmaze/"));
        return data;
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public class UserDefinedSource implements StreamTableSource<Row>, DefinedProctimeAttribute {

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public TableSchema getTableSchema() {
            String[] names = new String[]{"id", "number", "autoAddTime"};
            TypeInformation[] types = new TypeInformation[]{Types.INT(), Types.INT(), Types.SQL_TIMESTAMP()};
            return new TableSchema(names, types);
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
        public String explainSource() {
            return "UserDefinedSource";
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
        public TypeInformation<Row> getReturnType() {
            String[] names = new String[]{"id", "number"};
            TypeInformation[] types = new TypeInformation[]{Types.INT(), Types.INT()};
            return Types.ROW(names, types);
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
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {

            String[] names = new String[]{"id", "number"};
            TypeInformation[] types = new TypeInformation[]{Types.INT(), Types.INT()};
            DataStream<Row> inputStream = execEnv.addSource(new SourceFunction<Row>() {
                /**
                 * github地址: https://github.com/intsmaze
                 * 博客地址：https://www.cnblogs.com/intsmaze/
                 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
                 *
                 * @auther: intsmaze(刘洋)
                 * @date: 2020/10/15 18:33
                 */
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
            }).returns(Types.ROW(names, types));

            return inputStream;
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
        public String getProctimeAttribute() {
            return "autoAddTime";
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
    @Test
    public void testProcessTimeSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.registerTableSource("test", new UserDefinedSource());

        String sqlQuery = "SELECT id," +
                "sum(number) " +
                ",TUMBLE_START(autoAddTime, INTERVAL '14' SECOND) AS wStart" +
                ", TUMBLE_END(autoAddTime, INTERVAL '14' SECOND) AS wEnd " +
                ", TUMBLE_PROCTIME(autoAddTime, INTERVAL '14' SECOND) AS wEnd " +
                "FROM test " +
                "GROUP BY TUMBLE(autoAddTime, INTERVAL '14' SECOND), id ";

        Table result = tableEnv.sqlQuery(sqlQuery);
        tableEnv.toRetractStream(result, Row.class).print("toRetractStream");
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
    public class UserRowtimeSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public TypeInformation<Row> getReturnType() {
            String[] names = new String[]{"uid", "user", "addTime", "url"};
            TypeInformation[] types =
                    new TypeInformation[]{Types.INT(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING()};
            return Types.ROW(names, types);
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
        public TableSchema getTableSchema() {
            String[] names = new String[]{"uid", "user", "addTime", "url"};
            TypeInformation[] types =
                    new TypeInformation[]{Types.INT(), Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING()};
            return new TableSchema(names, types);
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
        public String explainSource() {
            return "UserRowtimeSource";
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
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
            DataStream<Row> dataStream = null;
            try {
                dataStream = execEnv.fromCollection(getRowDataSetTimeAttributes());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return dataStream;
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
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
                    "addTime",
                    new ExistingField("addTime"),
                    new AscendingTimestamps());
            List<RowtimeAttributeDescriptor> listRowtimeAttrDesc = Collections.singletonList(rowtimeAttrDescr);
            return listRowtimeAttrDesc;
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
    @Test
    public void testEventTimeSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        tableEnv.registerTableSource("clicks", new UserRowtimeSource());

        String sqlQuery = "SELECT user AS name,min(uid)," +
                "count(1) " +
                ",TUMBLE_START(addTime, INTERVAL '1' HOUR)" +
                " ,TUMBLE_END(addTime, INTERVAL '1' HOUR)  " +
                " ,TUMBLE_ROWTIME(addTime, INTERVAL '1' HOUR)" +
                "FROM clicks " +
                "GROUP BY TUMBLE(addTime, INTERVAL '1' HOUR), user ";

        tableEnv.toRetractStream(tableEnv.sqlQuery(sqlQuery), Row.class).print();

        env.execute();
    }

}
