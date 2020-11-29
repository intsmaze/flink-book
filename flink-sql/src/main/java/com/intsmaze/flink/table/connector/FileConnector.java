package com.intsmaze.flink.table.connector;

import com.intsmaze.flink.table.PrepareData;
import com.intsmaze.flink.table.bean.Person;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class FileConnector {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testTableSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        StreamTableDescriptor connect = tableEnv.connect(new FileSystem()
                .path("file:///home/intsmaze/flink/table/file-connector.csv")
        );
        connect = connect.withFormat(new Csv()
                .field("name", Types.STRING())
                .field("age", Types.LONG())
                .field("city", Types.STRING())
                .ignoreFirstLine()
                .ignoreParseErrors()
        );

        connect = connect.withSchema(new Schema()
                .field("name", "VARCHAR")
                .field("age", "BIGINT")
                .field("city", "VARCHAR")
        );

        connect = connect.inAppendMode();

        connect.registerTableSource("CsvTable");

        Table csvResult = tableEnv.sqlQuery("SELECT * FROM CsvTable WHERE age > 20");

        tableEnv.toAppendStream(csvResult, Row.class).print();
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
    public void testTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        StreamTableDescriptor connect = tableEnv.connect(new FileSystem()
                .path("file:///home/intsmaze/flink/table/file-connector/")
        );

        connect = connect.withFormat(new Csv()
                .field("name", Types.STRING())
                .field("city", Types.STRING())
                .fieldDelimiter(",")
        );

        connect = connect.withSchema(new Schema()
                .field("name", "VARCHAR")
                .field("city", "VARCHAR")
        );

        connect = connect.inAppendMode();

        connect.registerTableSink("CsvTable");


        List<Person> clicksData = PrepareData.getPersonData();
        DataStream<Person> dataStream = env.fromCollection(clicksData);

        tableEnv.registerDataStream("Person", dataStream, "name,age,city");
        tableEnv.sqlUpdate("INSERT INTO CsvTable SELECT name,city FROM Person WHERE age <20 ");

        env.execute();
    }


}
