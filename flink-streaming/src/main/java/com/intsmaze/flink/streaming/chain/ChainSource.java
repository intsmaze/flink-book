package com.intsmaze.flink.streaming.chain;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class ChainSource extends RichSourceFunction<Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    int sleep = 30000;

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        String subtaskName = getRuntimeContext().getTaskNameWithSubtasks();
        String info = "source操作所属子任务名称:";
        Tuple2 tuple2 = new Tuple2("185XXX", 899);
        ctx.collect(tuple2);
        System.out.println(info + subtaskName + ",元素:" + tuple2);
        Thread.sleep(sleep);

        tuple2 = new Tuple2("155XXX", 1199);
        ctx.collect(tuple2);
        System.out.println(info + subtaskName + ",元素:" + tuple2);
        Thread.sleep(sleep);

        tuple2 = new Tuple2("138XXX", 19);
        ctx.collect(tuple2);
        System.out.println(info + subtaskName + ",元素:" + tuple2);
        Thread.sleep(sleep);
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
