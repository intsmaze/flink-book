package com.intsmaze.flink.table.bean;

import com.intsmaze.flink.table.util.TimeStampUtils;

import java.sql.Timestamp;
import java.text.ParseException;

/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class ClickBean {

    public String user;

    public Timestamp time;

    public String url;

    public int id;

    public ClickBean() {
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public ClickBean(int id, String user, String url, String time) throws ParseException {
        this.user = user;
        this.url = url;
        this.time = TimeStampUtils.stringToTime(time);
        this.id = id;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }


    @Override
    public String toString() {
        return "ClickBean{" +
                "user='" + user + '\'' +
                ", time=" + time +
                ", url='" + url + '\'' +
                '}';
    }
}
