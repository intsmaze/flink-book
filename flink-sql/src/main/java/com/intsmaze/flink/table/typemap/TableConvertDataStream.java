package com.intsmaze.flink.table.typemap;

import com.intsmaze.flink.table.bean.OrderBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class TableConvertDataStream {


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
        env.setParallelism(1);

        DataStream<Tuple3<Long, String, Integer>> order = env.fromCollection(Arrays.asList(
                new Tuple3<Long, String, Integer>(1L, "手机", 1899),
                new Tuple3<Long, String, Integer>(1L, "电脑", 8888),
                new Tuple3<Long, String, Integer>(3L, "平板", 899)));

        tableEnv.registerDataStream("table_order", order, "user,product,amount");


        Table resultRow = tableEnv
                .sqlQuery("SELECT user,product,amount FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultRow, Row.class).print("Row Type: ");


        Table resultAtomic = tableEnv
                .sqlQuery("SELECT product FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultAtomic, String.class).print("Atomic Type: ");


        TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(
                Types.STRING(), Types.INT());
        Table resultTuple = tableEnv
                .sqlQuery("SELECT product,amount FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultTuple, tupleType).print("Tuple Type: ");


        Table resultPojo = tableEnv
                .sqlQuery("SELECT user,product,amount FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultPojo, OrderBean.class).print("Pojo Type: ");

        DataStream<Tuple2<Boolean, Row>> retract = tableEnv.toRetractStream(resultRow, Row.class);
        retract.print("Retract Row Type: ");

        env.execute();
    }
}
