package com.intsmaze.flink.dataset.connector.source;

import com.intsmaze.flink.dataset.bean.Trade;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;

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
public class CollectionSource {


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "file:///intsmaze/to/textfile";
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        DataSet<String> textInputFormat = env.readFile(format, filePath);

        DataSet<String> localLines = env.readTextFile("file:///intsmaze/to/textfile");

        DataSet<String> hdfsLines = env.readTextFile("hdfs://name-node-Host:Port/intsmaze/to/textfile");

        DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///intsmaze/CSV/file")
                .includeFields("10010")
                .includeFields(true, false, false, true, false)
                .types(String.class, Double.class);

        DataSet<Trade> csvInput1 = env.readCsvFile("hdfs:///intsmaze/CSV/file")
                .pojoType(Trade.class, "name", "age", "city");

        DataSet<String> value = env.fromElements("flink", "strom", "spark", "stream");

        List<String> list = new ArrayList<>();
        list.add("HashMap");
        list.add("List");
        env.fromCollection(list).print();

        env.fromElements("Flink", "intsmaze").print();

        env.generateSequence(100, 101).print();
    }
}
