package com.intsmaze.flink.streaming.state.savepoint;

import com.intsmaze.flink.streaming.state.operator.CheckpointedMapTemplate;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class SavePointedTemplate {

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
        env.enableCheckpointing(20000);

        String path = "file:///home/intsmaze/flink/checkpoint";
        StateBackend stateBackend = new RocksDBStateBackend(path);
        env.setStateBackend(stateBackend);

        DataStream<Long> sourceStream =
                env.addSource(new CheckpointedMapTemplate.CustomSource());

        sourceStream.map(new CheckpointedMapTemplate(false, false))
                .uid("map-id")
                .print();

        env.execute("Intsmaze SavePointedTemplate");
    }
}