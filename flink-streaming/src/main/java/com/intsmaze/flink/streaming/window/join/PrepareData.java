package com.intsmaze.flink.streaming.window.join;

import java.text.ParseException;
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
public class PrepareData {

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static List<ClickBean> getClicksData() throws ParseException {
        List<ClickBean> clickList = new ArrayList();
        clickList.add(new ClickBean(1, "张三", "./intsmaze", "2019-07-28 12:00:00"));
        clickList.add(new ClickBean(2, "李四", "./flink", "2019-07-28 12:05:05"));
        clickList.add(new ClickBean(3, "张三", "./stream", "2019-07-28 12:45:08"));
        clickList.add(new ClickBean(4, "李四", "./intsmaze", "2019-07-28 13:01:00"));
        clickList.add(new ClickBean(5, "王五", "./flink", "2019-07-28 13:04:00"));
        return clickList;
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static List<Trade> getTradeData() throws ParseException {
        List<Trade> tradeList = new ArrayList();
        tradeList.add(new Trade("张三", 38, "安卓手机", "2019-07-28 12:20:00"));
        tradeList.add(new Trade("王五", 45, "苹果手机", "2019-07-28 12:30:00"));
        tradeList.add(new Trade("张三", 18, "台式机", "2019-07-28 13:20:00"));
        tradeList.add(new Trade("王五", 23, "笔记本", "2019-07-28 13:58:00"));
        return tradeList;
    }
}
