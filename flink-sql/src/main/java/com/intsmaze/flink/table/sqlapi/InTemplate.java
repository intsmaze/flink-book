//package com.intsmaze.flink.table.sqlapi;
//
//import com.intsmaze.flink.table.PrepareData;
//import com.intsmaze.flink.table.bean.Person;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.junit.Test;
//
//import java.util.List;
//
///**
// * github地址: https://github.com/intsmaze
// * 博客地址：https://www.cnblogs.com/intsmaze/
// * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
// *
// * @auther: intsmaze(刘洋)
// * @date: 2020/10/15 18:33
// */
//public class InTemplate {
//
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Test
//    public void testDataSet() throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
//
//        List<Person> clicksData = PrepareData.getPersonData();
//        DataSet<Person> dataStream = env.fromCollection(clicksData);
//
//        tEnv.registerDataSet("Person", dataStream, "name,age,city");
//
//        Table table = tEnv.sqlQuery("SELECT * FROM Person where name IN ( SELECT name FROM Person WHERE age <30)");
//
//        DataSet<Row> result = tEnv.toDataSet(table, Row.class);
//        result.print();
//    }
//
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Test
//    public void testDataStream() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
//
//        List<Person> clicksData = PrepareData.getPersonData();
//        DataStream<Person> dataStream = env.fromCollection(clicksData);
//
//        tEnv.registerDataStream("Person", dataStream, "name,age,city");
//
//        DataStream<Person> tmpStream = dataStream.map(new MapFunction<Person, Person>() {
//            /**
//             * github地址: https://github.com/intsmaze
//             * 博客地址：https://www.cnblogs.com/intsmaze/
//             * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//             *
//             * @auther: intsmaze(刘洋)
//             * @date: 2020/10/15 18:33
//             */
//            @Override
//            public Person map(Person value) throws Exception {
//                System.out.println(value);
//                Thread.sleep(5000);
//                return value;
//            }
//        });
//        tEnv.registerDataStream("PersonTmp", tmpStream, "name,age,city");
//
//        Table table = tEnv.sqlQuery("SELECT * FROM Person WHERE name IN ( SELECT name FROM PersonTmp WHERE age <30)");
//
//        tEnv.toRetractStream(table, Row.class).print();
//        env.execute();
//    }
//
//
//}
