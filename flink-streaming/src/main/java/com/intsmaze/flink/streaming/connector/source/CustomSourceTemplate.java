package com.intsmaze.flink.streaming.connector.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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
public class CustomSourceTemplate implements SourceFunction<Long> {

    public static Logger LOG = LoggerFactory.getLogger(CustomSourceTemplate.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;
    private long counter = 0;

    private long sleepTime;

    public CustomSourceTemplate(long sleepTime) {
        this.sleepTime = sleepTime;
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
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(counter);
            System.out.println("send data :" + counter);
            LOG.info("send data :" + counter);
            counter++;
            Thread.sleep(sleepTime);
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
    @Override
    public void cancel() {
        LOG.warn("接收到取消任务的命令.......................");
        isRunning = true;
    }

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
        DataStream<Long> inputStream =
                env.addSource(new CustomSourceTemplate(100))
                        .setParallelism(1);

        DataStream<Long> inputStream1 = inputStream
                .map((Long values) -> {
                    return values + System.currentTimeMillis();
                });
        inputStream1.print();

        env.execute("Intsmaze Custom Source");
    }
}