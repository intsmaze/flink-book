package com.intsmaze.flink.streaming.bean;

import java.text.SimpleDateFormat;
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
public class EventSchemaBean {

    public int userId;

    public String date;

    public long timeStamp;

    public List<String> list = new ArrayList<String>();

    public EventSchemaBean() {
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public EventSchemaBean(int userId, long timeStamp) {
        this.userId = userId;
        this.timeStamp = timeStamp;
        SimpleDateFormat aDate = new SimpleDateFormat("HH:mm:ss:SSS");
        this.date = aDate.format(timeStamp);
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }


    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    @Override
    public String toString() {
        return "EventSchemaBean{" +
                "userId=" + userId +
                ", date='" + date + '\'' +
                ", timeStamp=" + timeStamp +
                ", list=" + list +
                '}';
    }
}