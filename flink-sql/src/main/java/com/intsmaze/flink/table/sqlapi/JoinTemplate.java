//package com.intsmaze.flink.table.sqlapi;
//
//import com.intsmaze.flink.table.PrepareData;
//import com.intsmaze.flink.table.bean.ClickBean;
//import com.intsmaze.flink.table.bean.Person;
//import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.text.ParseException;
//import java.util.ArrayList;
//import java.util.Collections;
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
//public class JoinTemplate {
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
//        List<ClickBean> clicksData = PrepareData.getClicksData();
//        DataSet<ClickBean> clicksStream = env.fromCollection(clicksData);
//        tEnv.registerDataSet("Clicks", clicksStream, "user,time,url");
//
//        List<Person> personData = PrepareData.getPersonData();
//        DataSet<Person> personStream = env.fromCollection(personData);
//        tEnv.registerDataSet("Person", personStream, "name,age,city");
//
//        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks INNER JOIN Person ON name=user"), Row.class).print("INNER JOIN");
//
//        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks LEFT JOIN Person ON name=user"), Row.class).print("LEFT JOIN");
//        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks RIGHT JOIN Person ON name=user"), Row.class).print("RIGHT JOIN");
//        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks FULL JOIN Person ON name=user"), Row.class).print("FULL JOIN");
//
//        env.execute();
//    }
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
//        env.setParallelism(1);
//
//        List<ClickBean> clicksData = PrepareData.getClicksData();
//        DataStream<ClickBean> clicksStream = env.fromCollection(clicksData);
//        clicksStream = clicksStream.map((MapFunction<ClickBean, ClickBean>) value -> {
//            Thread.sleep(2000);
//            return value;
//        });
//        tEnv.registerDataStream("Clicks", clicksStream, "user,time,url");
//
//        List<Person> personData = PrepareData.getPersonData();
//        Collections.shuffle(personData);
//        DataStream<Person> personStream = env.fromCollection(personData);
//        personStream = personStream.map((MapFunction<Person, Person>) value -> {
//            Thread.sleep(4000);
//            return value;
//        });
//        tEnv.registerDataStream("Person", personStream, "name,age,city");
//
//
//        tEnv.toAppendStream(tEnv.sqlQuery("SELECT * FROM Clicks INNER JOIN Person ON name=user"), Row.class).print("INNER JOIN");
//        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks LEFT JOIN Person ON name=user"), Row.class).print("LEFT JOIN");
//        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks RIGHT JOIN Person ON name=user"), Row.class).print("RIGHT JOIN");
//        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks FULL JOIN Person ON name=user"), Row.class).print("FULL JOIN");
//
//        env.execute();
//    }
//
//
//}
