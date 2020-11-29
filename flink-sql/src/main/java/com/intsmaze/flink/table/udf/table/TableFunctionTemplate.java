package com.intsmaze.flink.table.udf.table;

import com.intsmaze.flink.table.bean.OrderBean;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class TableFunctionTemplate {


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<OrderBean> input = env.fromElements(
                new OrderBean(1L, "beer#intsmaze", 3),
                new OrderBean(1L, "flink#intsmaze", 4),
                new OrderBean(3L, "rubber#intsmaze", 2));

        tableEnv.registerDataSet("orderTable", input, "user,product,amount");

//        tableEnv.registerFunction("splitFunction", new SplitTable("#"));

        tableEnv.registerFunction("splitFunction", new SplitTableTypeInfor("#"));

        Table sqlCrossResult = tableEnv.sqlQuery("SELECT user,product,amount,word, length FROM orderTable, " +
                "LATERAL TABLE(splitFunction(product)) AS T(word, length)");

        Table sqlLeftResult = tableEnv.sqlQuery("SELECT user,product,amount,word, length FROM orderTable LEFT JOIN LATERAL TABLE(splitFunction(product)) AS T(word, length) ON TRUE");

        tableEnv.toDataSet(sqlCrossResult, Row.class).print("CROSS JOIN");
        tableEnv.toDataSet(sqlLeftResult, Row.class).print("LEFT JOIN ");

        env.execute("TableFunctionTemplate");
    }
}
