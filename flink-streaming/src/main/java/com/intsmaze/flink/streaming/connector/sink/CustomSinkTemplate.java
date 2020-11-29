package com.intsmaze.flink.streaming.connector.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class CustomSinkTemplate {


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
        env.setParallelism(1);
        DataStream<Tuple2<Long, String>> streamSource = env.fromElements(
                new Tuple2<Long, String>(1L, "intsmaze"),
                new Tuple2<Long, String>(2L, "Flink"));

        streamSource.addSink(new CustomSink());
        streamSource.addSink(new CustomRichSink());
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
    private static class CustomSink implements SinkFunction<Tuple2<Long, String>> {

        private static final Logger LOGGER = LoggerFactory.getLogger(CustomSink.class);

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void invoke(Tuple2<Long, String> value, Context context) throws Exception {
            Properties druidP = new Properties();
            druidP.put("driverClassName", "com.mysql.jdbc.Driver");
            druidP.put("url", "jdbc:mysql://localhost:3306/test");
            druidP.put("username", "root");
            druidP.put("password", "intsmaze");
            DruidDataSource dataSource = (DruidDataSource) DruidDataSourceFactory
                    .createDataSource(druidP);
            Connection connection = dataSource.getConnection();

            String sql1 = "insert into flink_table(id,name) values(?,?)";
            PreparedStatement statement = connection.prepareStatement(sql1);
            statement.setLong(1, value.f0);
            statement.setString(2, value.f1);
            LOGGER.info("插入数据:{}", value);
            int rows = statement.executeUpdate();
            statement.close();
            dataSource.close();
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
    private static class CustomRichSink extends RichSinkFunction<Tuple2<Long, String>> {

        private static final Logger LOGGER = LoggerFactory.getLogger(CustomRichSink.class);

        private DruidDataSource dataSource;

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            LOGGER.info(">>>>>>>>>>>>>>.初始化资源的连接");
            Properties druidP = new Properties();
            druidP.put("driverClassName", "com.mysql.jdbc.Driver");
            druidP.put("url", "jdbc:mysql://localhost:3306/test");
            druidP.put("username", "root");
            druidP.put("password", "intsmaze");
            dataSource = (DruidDataSource) DruidDataSourceFactory
                    .createDataSource(druidP);
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
        public void close() {
            LOGGER.info(">>>>>>>>>>>>>>.释放连接的资源");
            dataSource.close();
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
        public void invoke(Tuple2<Long, String> value, Context context) throws SQLException {
            Connection connection = dataSource.getConnection();
            String sql1 = "insert into flink_table(id,name) values(?,?)";
            PreparedStatement statement = connection.prepareStatement(sql1);
            statement.setLong(1, value.f0);
            statement.setString(2, value.f1);
            LOGGER.info("插入数据:{}", value);
            int rows = statement.executeUpdate();
            statement.close();
        }
    }


}
