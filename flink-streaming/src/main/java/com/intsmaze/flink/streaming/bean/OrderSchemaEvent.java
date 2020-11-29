package com.intsmaze.flink.streaming.bean;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class OrderSchemaEvent {

    public int userId;

    public String url;

    public String deviceType;

    public long count = 1;

    public OrderSchemaEvent() {
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public OrderSchemaEvent(int userId, String url, String deviceType) {
        this.userId = userId;
        this.url = url;
        this.deviceType = deviceType;
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

    @Override
    public String toString() {
        return "ClickSchemaEvent{" +
                "userId=" + userId +
                ", url='" + url + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", count=" + count +
                '}';
    }
}