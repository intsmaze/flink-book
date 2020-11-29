package com.intsmaze.flink.streaming.bean;

import java.text.SimpleDateFormat;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class ClickSchemaEvent {

    public int userId;

    public String url;

    public String date;

    public long timeStamp;

    public String deviceType;

    public long count = 1;

    public ClickSchemaEvent() {
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public ClickSchemaEvent(int userId, String url, long timeStamp, String deviceType) {
        this.userId = userId;
        this.url = url;
        this.timeStamp = timeStamp;
        this.deviceType = deviceType;
        SimpleDateFormat aDate = new SimpleDateFormat("HH:mm:ss");
        this.date = aDate.format(timeStamp);
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
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

    @Override
    public String toString() {
        return "ClickSchemaEvent{" +
                "userId=" + userId +
                ", date=" + date +
                ", url='" + url + '\'' +
                ", timeStamp=" + timeStamp +
                ", deviceType='" + deviceType + '\'' +
                ", count=" + count +
                '}';
    }
}