package com.intsmaze.flink.streaming.window.source;

import com.intsmaze.flink.streaming.window.util.TimeUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class SourceForWindow implements SourceFunction<Tuple3<String, Integer, String>> {

    public static Logger LOG = LoggerFactory.getLogger(SourceForWindow.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private long sleepTime;

    private Boolean stopSession = false;

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public SourceForWindow(long sleepTime, Boolean stopSession) {
        this.sleepTime = sleepTime;
        this.stopSession = stopSession;
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public SourceForWindow(long sleepTime) {
        this.sleepTime = sleepTime;
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
    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            String word = WORDS[count % WORDS.length];
            String time = TimeUtils.getHHmmss(System.currentTimeMillis());
            Tuple3<String, Integer, String> tuple2 = Tuple3.of(word, count, time);
            ctx.collect(tuple2);
            LOG.info("send data :" + tuple2);
            System.out.println("send data :" + tuple2);
            if (stopSession && count == WORDS.length) {
                Thread.sleep(10000);
            } else {
                Thread.sleep(sleepTime);
            }
            count++;
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
        isRunning = false;
    }


    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static final String[] WORDS = new String[]{
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "java",
            "flink",
            "flink",
            "flink",
            "intsmaze",
            "intsmaze",
            "hadoop",
            "hadoop",
            "spark"
    };
}