package com.intsmaze.flink.table.connector;

import com.intsmaze.flink.table.PrepareData;
import com.intsmaze.flink.table.bean.Person;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class JdbcSinkTemplate {

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
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        List<Person> clicksData = PrepareData.getPersonData();
        DataStream<Person> dataStream = env.fromCollection(clicksData);

        tableEnv.registerDataStream("Person", dataStream, "name,age,city");

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://127.0.0.1:3306/test")
                .setPassword("intsmaze")
                .setUsername("root")
                .setQuery("INSERT INTO jdbc_test (name,age,city) VALUES (?,?,?)")
                .setParameterTypes(STRING_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)
                .build();

        tableEnv.registerTableSink(
                "jdbcOutputTable",
                new String[]{"name", "age", "city"},
                new TypeInformation[]{Types.STRING, Types.LONG, Types.STRING},
                sink);

        tableEnv.sqlUpdate("INSERT INTO jdbcOutputTable SELECT name,age,city FROM Person WHERE age <20 ");

        env.execute();
    }
}
