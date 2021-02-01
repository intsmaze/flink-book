//package com.intsmaze.flink.table.udf.aggre;
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.functions.AggregateFunction;
//import org.apache.flink.types.Row;
//
//import java.util.ArrayList;
//import java.util.Iterator;
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
//public class AggregateFunctionTemplate extends AggregateFunction<Double, AccumulatorBean> {
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
//    @Override
//    public AccumulatorBean createAccumulator() {
//        return new AccumulatorBean();
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
//    @Override
//    public Double getValue(AccumulatorBean acc) {
//        return acc.totalPrice / acc.totalNum;
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
//    public void accumulate(AccumulatorBean acc, double price, int num) {
//        acc.totalPrice += price * num;
//        acc.totalNum += num;
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
//    public void retract(AccumulatorBean acc, long iValue, int iWeight) {
//        acc.totalPrice -= iValue * iWeight;
//        acc.totalNum -= iWeight;
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
//    public void merge(AccumulatorBean acc, Iterable<AccumulatorBean> it) {
//        Iterator<AccumulatorBean> iter = it.iterator();
//        while (iter.hasNext()) {
//            AccumulatorBean a = iter.next();
//            acc.totalNum += a.totalNum;
//            acc.totalPrice += a.totalPrice;
//        }
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
//    public void resetAccumulator(AccumulatorBean acc) {
//        acc.totalNum = 0;
//        acc.totalPrice = 0L;
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
//    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
//
//        List<Row> dataList = new ArrayList<>();
//        dataList.add(Row.of("张三", "可乐", 20.0D, 4));
//        dataList.add(Row.of("张三", "果汁", 10.0D, 4));
//        dataList.add(Row.of("李四", "咖啡", 10.0D, 2));
//
//        DataSource<Row> rowDataSource = env.fromCollection(dataList);
//
//        tEnv.registerDataSet("orders", rowDataSource, "user,name,price, num");
//
//        tEnv.registerFunction("custom_aggregate", new AggregateFunctionTemplate());
//
//        Table sqlResult = tEnv.sqlQuery("SELECT user, custom_aggregate(price, num)  FROM orders GROUP BY user");
//
//        DataSet<Row> result = tEnv.toDataSet(sqlResult, Row.class);
//        result.print("result");
//        env.execute();
//    }
//}
//
//
//
//
