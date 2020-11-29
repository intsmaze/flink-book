package com.intsmaze.flink.table.sqlapi;

import com.intsmaze.flink.table.bean.Person;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.junit.Test;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class InsertIntoTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void testDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setParallelism(1);

        DataSet<Person> input = env.fromElements(
                new Person("张三", 10, "上海"),
                new Person("李四", 20, "上海"),
                new Person("王五", 3, "北京"),
                new Person("赵六", 2, "北京"));
        tableEnv.registerDataSet("Person", input);


        TableSink csvSink = new CsvTableSink("///home/intsmaze/flink/table/person-set.txt", "|");
        String[] fieldNames = {"name", "age", "city"};

        TypeInformation[] fieldTypes = {Types.STRING(), Types.LONG(), Types.STRING()};
        tableEnv.registerTableSink("SinkPerson", fieldNames, fieldTypes, csvSink);

        tableEnv.sqlUpdate("INSERT INTO SinkPerson " +
                "SELECT name,age,city FROM Person");

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
    public void testDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setParallelism(1);

        DataStream<Person> input = env.fromElements(
                new Person("张三", 10, "上海"),
                new Person("李四", 20, "上海"),
                new Person("王五", 3, "北京"),
                new Person("赵六", 2, "北京"));
        tableEnv.registerDataStream("Person", input);

        TableSink csvSink = new CsvTableSink("///home/intsmaze/flink/table/person-stream.txt", "|");
        String[] fieldNames = {"name", "age", "city"};

        TypeInformation[] fieldTypes = {Types.STRING(), Types.LONG(), Types.STRING()};
        tableEnv.registerTableSink("SinkPerson", fieldNames, fieldTypes, csvSink);


        tableEnv.sqlUpdate("INSERT INTO SinkPerson " +
                "SELECT name,age,city FROM Person");
        env.execute();
    }

}
