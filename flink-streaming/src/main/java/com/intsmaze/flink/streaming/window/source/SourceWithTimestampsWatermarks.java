package com.intsmaze.flink.streaming.window.source;

import com.intsmaze.flink.streaming.window.time.bean.EventBean;
import com.intsmaze.flink.streaming.window.util.TimeUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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
public class SourceWithTimestampsWatermarks implements SourceFunction<EventBean> {

    public static Logger LOG = LoggerFactory.getLogger(SourceWithTimestampsWatermarks.class);

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    private int counter = 0;

    private long sleepTime;

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public SourceWithTimestampsWatermarks(long sleepTime) {
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
    public void run(SourceContext<EventBean> ctx) throws Exception {
        while (isRunning) {
            if (counter >= 16) {
                isRunning = false;
            } else {
                EventBean bean = Data.BEANS[counter];
                ctx.collectWithTimestamp(bean, bean.getTime());
                String time = TimeUtils.getHHmmss(System.currentTimeMillis());
                System.out.println("send 元素内容 : [" + bean + " ] now time:" + time);
                if (bean.getList().get(0).indexOf("late") < 0) {
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
                if (bean.getList().get(0).indexOf("nosleep") < 0) {
                    Thread.sleep(sleepTime);
                }
            }
            counter++;
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


}

