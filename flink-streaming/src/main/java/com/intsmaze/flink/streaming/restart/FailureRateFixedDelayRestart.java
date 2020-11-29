package com.intsmaze.flink.streaming.restart;

import com.intsmaze.flink.streaming.state.key.KeyStateBase;
import com.intsmaze.flink.streaming.state.key.ValueStateFlatMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
@Deprecated
public class FailureRateFixedDelayRestart {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String exceptionType = params.get("exceptionType");
        String restartStrategy = params.get("restartStrategy");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        if (restartStrategy.equals("failureRateRestart")) {
            env.setRestartStrategy(RestartStrategies.failureRateRestart(
                    3,
                    Time.of(5, TimeUnit.MINUTES),
                    Time.of(10, TimeUnit.SECONDS)
            ));
        } else if (restartStrategy.equals("fixedDelayRestart")) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    2,
                    Time.of(10, TimeUnit.SECONDS)
            ));
        } else if (restartStrategy.equals("noRestart")) {
            env.setRestartStrategy(RestartStrategies.noRestart());
        } else if (restartStrategy.equals("enableCheckpoint")) {
            env.enableCheckpointing(2000);
            String url = "file:///home/intsmaze/flink/check/rocksDB/";
            RocksDBStateBackend stateBackend = new RocksDBStateBackend(url, true);
            env.setStateBackend(stateBackend);
            env.getConfig().setExecutionRetryDelay(10000);
            env.getConfig().setNumberOfExecutionRetries(3);
        } else if (restartStrategy.equals("fallBackRestart")) {
            env.setRestartStrategy(RestartStrategies.fallBackRestart());
        }
        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = KeyStateBase.before(env);
        if (exceptionType.equals("Serverdown")) {
            keyedStream.print();
        } else if (exceptionType.equals("programException")) {
            keyedStream.flatMap(new ValueStateFlatMap())
                    .map(new MapFunction<Tuple2<Integer, Integer>, String>() {
                        @Override
                        public String map(Tuple2<Integer, Integer> value) {
                            Random ra = new Random();
                            int num = ra.nextInt(4) + 1;
                            if (num == 3) {
                                int i = 1 / 0;
                            }
                            return value.toString();
                        }
                    }).print();
        }
        env.execute("exceptionType");
    }

}
