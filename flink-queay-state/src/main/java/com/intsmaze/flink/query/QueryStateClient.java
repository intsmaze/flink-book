package com.intsmaze.flink.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class QueryStateClient {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(final String[] args) throws Exception {
        String jobId = "488266c6a81ca05fe7557ad7865aa789";

        QueryableStateClient client = new QueryableStateClient("intsmaze-202", 9069);

        ValueStateDescriptor<Tuple2<Integer, Integer>> stateDescriptor =
                new ValueStateDescriptor<>(
                        "ValueState",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        }));

        while (true) {
            ValueState<Tuple2<Integer, Integer>> valueState =
                    getState(jobId, client, stateDescriptor);
            System.out.println("查询作业中指定key的状态值为：" + valueState.value());
            Thread.sleep(1000L);
        }
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    private static ValueState<Tuple2<Integer, Integer>> getState(
            String jobId,
            QueryableStateClient client,
            ValueStateDescriptor<Tuple2<Integer, Integer>> stateDescriptor) throws InterruptedException, ExecutionException {

        CompletableFuture<ValueState<Tuple2<Integer, Integer>>> resultFuture =
                client.getKvState(
                        JobID.fromHexString(jobId),
                        "query-value-state-name",
                        2,
                        BasicTypeInfo.INT_TYPE_INFO,
                        stateDescriptor);
        return resultFuture.get();
    }
}
