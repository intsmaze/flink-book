package com.intsmaze.flink.streaming.connector.source;

import com.google.gson.Gson;
import com.intsmaze.flink.streaming.bean.SchemaBean;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class KafkaKeyedDeserializationSchema implements KeyedDeserializationSchema<Tuple2<String, SchemaBean>> {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public Tuple2<String, SchemaBean> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        Gson gson = new Gson();
        String key = gson.fromJson(new String(messageKey), String.class);
        SchemaBean value = gson.fromJson(new String(message), SchemaBean.class);
        return Tuple2.of(key, value);
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
    public boolean isEndOfStream(Tuple2<String, SchemaBean> nextElement) {
        return false;
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
    public TypeInformation<Tuple2<String, SchemaBean>> getProducedType() {
        return new TypeHint<Tuple2<String, SchemaBean>>() {
        }.getTypeInfo();
    }
}
