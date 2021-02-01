//package com.intsmaze.flink.table;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.Types;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.sources.CsvTableSource;
//import org.apache.flink.table.sources.TableSource;
//import org.apache.flink.types.Row;
//
///**
// * github地址: https://github.com/intsmaze
// * 博客地址：https://www.cnblogs.com/intsmaze/
// * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
// *
// * @auther: intsmaze(刘洋)
// * @date: 2020/10/15 18:33
// */
//public class TableApiTemplate {
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//
//        String[] fieldNames = {"name", "age", "city"};
//
//        TypeInformation[] fieldTypes = {Types.STRING(), Types.INT(), Types.STRING()};
//        TableSource csvSource = new CsvTableSource("///home/intsmaze/flink/table/data", fieldNames, fieldTypes);
//
//        tableEnv.registerTableSource("Person", csvSource);
//
//        Table table = tableEnv.scan("Person").filter("age >30")
//                .groupBy("name").select("name,count(1)");
//
//        tableEnv.toRetractStream(table, Row.class).print();
//
//        env.execute();
//    }
//}