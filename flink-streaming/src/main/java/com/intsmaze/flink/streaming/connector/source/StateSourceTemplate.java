package com.intsmaze.flink.streaming.connector.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class StateSourceTemplate extends RichParallelSourceFunction<Long>
        implements CheckpointedFunction {

    public static Logger LOG = LoggerFactory.getLogger(StateSourceTemplate.class);

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

        env.enableCheckpointing(10000);
        String url = "file:///home/intsmaze/flink/check/ListCheckpointedPoJo";
        StateBackend stateBackend = new RocksDBStateBackend(url, false);
        env.setStateBackend(stateBackend);
        env.setParallelism(2);
        DataStream<Long> inputStream = env
                .addSource(new StateSourceTemplate(true))
                .setParallelism(1);

        inputStream.print();

        env.execute("StateSourceTemplate");
    }


    private Long offset = 0L;

    private transient ListState<Long> checkpointedCount;

    private boolean isMakeError;

    public StateSourceTemplate(boolean isMakeError) {
        this.isMakeError = isMakeError;
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
    public void run(SourceContext<Long> ctx) throws InterruptedException {
        final Object lock = ctx.getCheckpointLock();

        while (true) {
            synchronized (lock) {
                LOG.info("{}{}{}", Thread.currentThread().getName(), ":发送数据:", offset);
                ctx.collect(offset);
                offset += 1;
                if (isMakeError) {
                    int seconds = Calendar.getInstance().get(Calendar.SECOND);
                    if (seconds >= 50 && seconds <= 51) {
                        LOG.info("模拟程序异常状况");
                        int i = 1 / 0;
                    }
                }
            }
            Thread.sleep(1000);
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
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(offset);
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
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<>("offset", Long.class));

        if (context.isRestored()) {
            for (Long count : this.checkpointedCount.get()) {
                this.offset = count;
            }
        }
    }
}