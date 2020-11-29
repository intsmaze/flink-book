package com.intsmaze.flink.streaming.operator.rich;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class RichFunctionTemplate extends RichFlatMapFunction<Long, Long> {

    public static Logger LOGGER = LoggerFactory.getLogger(RichFunctionTemplate.class);

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
        env.setParallelism(2);

        DataStream<Long> streamSource = env.generateSequence(1, 100);
        DataStream<Long> dataStream = streamSource
                .flatMap(new RichFunctionTemplate())
                .name("intsmaze-flatMap");
        dataStream.print();

        env.execute("RichFunctionTemplate");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();
        String taskName = rc.getTaskName();
        String subtaskName = rc.getTaskNameWithSubtasks();
        int subtaskIndexOf = rc.getIndexOfThisSubtask();
        int parallel = rc.getNumberOfParallelSubtasks();
        int attemptNum = rc.getAttemptNumber();
        LOGGER.info("调用open方法,初始化资源信息..");
        LOGGER.info("调用open方法,任务名称:{}...带有子任务的任务名称：{}..并行子任务的标识：{}..当前任务的总并行度:{}", taskName, subtaskName, subtaskIndexOf, parallel);
        LOGGER.info("调用open方法,该任务因为失败进行重启的次数:{}", attemptNum);

    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public void flatMap(Long input, Collector<Long> out) throws Exception {
        Thread.sleep(1000);
        out.collect(input);
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public void close() {
        System.out.println("调用close方法 ----------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOGGER.info("调用close方法 -----------------------");
    }
}