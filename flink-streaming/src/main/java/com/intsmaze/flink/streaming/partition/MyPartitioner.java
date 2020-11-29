package com.intsmaze.flink.streaming.partition;

import org.apache.flink.api.common.functions.Partitioner;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class MyPartitioner implements Partitioner<String> {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public int partition(String key, int numPartitions) {
        if (key.indexOf("185") >= 0) {
            return 0;
        } else if (key.indexOf("155") >= 0) {
            return 1;
        } else {
            return 2;
        }
    }
}