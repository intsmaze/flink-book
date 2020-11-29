package com.intsmaze.flink.table.connector;

import com.intsmaze.flink.table.PrepareData;
import com.intsmaze.flink.table.bean.Person;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
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
public class KafkaConnector {


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

        StreamTableDescriptor connect = tableEnv.connect(new Kafka()
                .version("universal")
                .topic("flink-intsmaze")
                .property("zookeeper.connect", "192.168.19.201:2181")
                .property("bootstrap.servers", "192.168.19.201:9092")
        );

        connect = connect.withFormat(new Avro()
                .avroSchema(
                        "{" +
                                "  \"type\": \"record\"," +
                                "  \"name\": \"test\"," +
                                "  \"fields\" : [" +
                                "    {\"name\": \"name\", \"type\": \"string\"}," +
                                "    {\"name\": \"city\", \"type\": \"string\"}" +
                                "  ]" +
                                "}"
                )
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

        StreamTableDescriptor connect = tableEnv.connect(new Kafka()
                .version("universal")
                .topic("flink-intsmaze")
                .property("zookeeper.connect", "192.168.19.201:2181")
                .property("bootstrap.servers", "192.168.19.201:9092")
                .property("group.id", "testGroup")
                .startFromLatest()
        );

        connect = connect.withFormat(new Avro()
                .avroSchema(
                        "{" +
                                "  \"type\": \"record\"," +
                                "  \"name\": \"test\"," +
                                "  \"fields\" : [" +
                                "    {\"name\": \"name\", \"type\": \"string\"}," +
                                "    {\"name\": \"city\", \"type\": \"string\"}" +
                                "  ]" +
                                "}"
                )
        );

        connect = connect.withSchema(new Schema()
                .field("name", "VARCHAR")
                .field("city", "VARCHAR")
        );

        connect = connect.inAppendMode();

        connect.registerTableSource("CsvTable");

        Table csvResult = tableEnv.sqlQuery("SELECT * FROM CsvTable ");

        tableEnv.toAppendStream(csvResult, Row.class).print();

        env.execute();
    }
}