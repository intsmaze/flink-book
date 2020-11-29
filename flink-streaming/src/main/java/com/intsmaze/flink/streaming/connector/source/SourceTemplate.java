package com.intsmaze.flink.streaming.connector.source;

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
@Deprecated
public class SourceTemplate {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void tsetCollection() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> list = new ArrayList<>();
        list.add("HashMap");
        list.add("List");
        env.fromCollection(list).print();

        env.fromElements("Flink", "intsmaze").print();

        env.generateSequence(100, 101).print();

        env.execute("collection");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void tsetSocket() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env
                .socketTextStream("intsmaze-203", 9998);

        dataStream.print();

        env.execute("SocketSource");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void tset() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String filePath = "///home/intsmaze/flink/Source/TextFileSource.txt";
//        DataStream<String> dataStream = env.readTextFile(filePath);

        TextInputFormat format = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        DataStream<String> dataStream = env.readFile(format, filePath,
                FileProcessingMode.PROCESS_CONTINUOUSLY, 10000, typeInfo);

        dataStream.print();
        env.execute("TextFileSource");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void tsetOne() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filePath = "d:///home/intsmaze/flink/TextFileSource/1.txt";

        TextInputFormat format = new TextInputFormat(new Path(filePath));
        format.setCharsetName("UTF-8");
        format.setFilesFilter(FilePathFilter.createDefaultFilter());
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;


        DataStream<String> dataStream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10000, typeInfo);
        dataStream.print();
        env.execute("TextFileSource");
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Test
    public void tsetTwo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = "d:///home/intsmaze/flink/TextFileSource/1.txt";
        DataStream<String> dataStream = env.readTextFile(filePath);
        dataStream.writeAsText("///home/intsmaze/flink/sink-text-1.txt");
        env.execute("TextFileSource");
    }

}
