package com.intsmaze.flink.table.sqlapi;

import com.intsmaze.flink.table.bean.PersonUnnestBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class UnnestTemplate {

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
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        List<PersonUnnestBean> personList = new ArrayList();
        personList.add(new PersonUnnestBean("张三", 38, new String[]{"上海", "浦东新区"}));
        personList.add(new PersonUnnestBean("李四", 45, new String[]{"深圳", "福田区"}));

        DataStream<PersonUnnestBean> personStream = env.fromCollection(personList);

        tEnv.registerDataStream("Person", personStream, "name,age,city");

        tEnv.toRetractStream(tEnv.sqlQuery("SELECT name,age,area FROM Person CROSS JOIN UNNEST(city) AS t (area)"), Row.class).print("UNNEST");

        env.execute();
    }
}
