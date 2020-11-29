package com.intsmaze.flink.streaming.state.operator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class CheckpointedMapTemplate implements MapFunction<Long, String>,
        CheckpointedFunction {

    public static Logger LOG = LoggerFactory.getLogger(CheckpointedMapTemplate.class);

    private transient ListState<Long> checkpointedState;

    private LinkedList<Long> bufferedElements;

    private boolean isUnion;

    private boolean isError;

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public CheckpointedMapTemplate(boolean isUnion, boolean isError) {
        this.isUnion = isUnion;
        this.isError = isError;
        this.bufferedElements = new LinkedList<>();
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
    public String map(Long value) {
        int size = bufferedElements.size();
        if (size >= 10) {
            for (int i = 0; i < size - 9; i++) {
                Long poll = bufferedElements.poll();
            }
        }
        bufferedElements.add(value);
        if (isError) {
            int seconds = Calendar.getInstance().get(Calendar.SECOND);
            if (seconds >= 50 && seconds <= 51) {
                int i = 1 / 0;
            }
        }
        LOG.info("{} map data :{}", Thread.currentThread().getName(), bufferedElements);
        return "集合中第一个元素是:" + bufferedElements.getFirst() +
                "集合中最后一个元素是:" + bufferedElements.getLast() +
                " length is :" + bufferedElements.size();
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
        LOG.info(Thread.currentThread().getName() + ":" + context.getCheckpointId() + ":"
                + context.getCheckpointTimestamp() + "............................snapshotState");
        LOG.info("{} 快照编号{} 的元素为:{}", Thread.currentThread().getName()
                , context.getCheckpointId(), bufferedElements);
        checkpointedState.clear();
        checkpointedState.addAll(bufferedElements);
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
        ListStateDescriptor<Long> descriptor =
                new ListStateDescriptor<Long>(
                        "CheckpointedFunctionTemplate-ListState",
                        TypeInformation.of(new TypeHint<Long>() {
                        }));
        String threadName = Thread.currentThread().getName();
        if (isUnion) {
            checkpointedState = context.getOperatorStateStore().getUnionListState(descriptor);
        } else {
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        }
        if (context.isRestored()) {
            LOG.info("{} operator状态恢复", threadName);
            for (Long element : checkpointedState.get()) {
                bufferedElements.offer(element);
            }
        }
        LOG.info("{} operator状态初始化/恢复{}", threadName, bufferedElements);
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
        env.enableCheckpointing(10000);
        env.setParallelism(2);

        String path = "file:///home/intsmaze/flink/check/CheckpointedFunctionTemplate";
        FsStateBackend stateBackend = new FsStateBackend(path);
        env.setStateBackend(stateBackend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

        DataStream<Long> streamSource = env.addSource(new CustomSource())
                .setParallelism(1);
        DataStream<String> mapResult = streamSource
                .map(new CheckpointedMapTemplate(false, true));
        mapResult.print("输出结果");
        env.execute("Intsmaze CheckpointedFunctionTemplate");
    }


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static class CustomSource extends RichSourceFunction<Long> {

        public Logger LOG = LoggerFactory.getLogger(CustomSource.class);

        private static final long serialVersionUID = 1L;

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            Thread.sleep(10000);
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
            Long offset = 0L;
            while (true) {
                LOG.info("{}{}{}", Thread.currentThread().getName(), ":发送数据:", offset);
                ctx.collect(offset);
                offset += 1;
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
    }

}