package com.intsmaze.flink.streaming.param;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class ParamTemplate {


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {

        ParameterTool parameterFromJvm = ParameterTool.fromSystemProperties();

        ParameterTool parameterFromMain = ParameterTool.fromArgs(args);
        parameterFromMain.get("input");

        InputStream inputStream = ParamTemplate.class.getClassLoader()
                .getResourceAsStream("flink-param.properties");

        ParameterTool parameter = ParameterTool.fromPropertiesFile(inputStream);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Configuration conf = new Configuration();
        conf.setLong("limit", 16);
        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.setGlobalJobParameters(conf);

        DataStream<Long> dataStream = env.generateSequence(1, 20);

        dataStream.filter(new FilterJobParameters())
                .print("JobParameters stream is :");

        env.execute("ParamTemplate intsmaze");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    private static class FilterJobParameters extends RichFilterFunction<Long> {

        protected long limit;

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
            ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
            ExecutionConfig.GlobalJobParameters globalParams = executionConfig.getGlobalJobParameters();
            Configuration globConf = (Configuration) globalParams;
            limit = globConf.getLong("limit", 0);
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
        public boolean filter(Long value) {
            return value > limit;
        }
    }
}