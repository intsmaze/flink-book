package com.intsmaze.flink.streaming.window.join;

import com.intsmaze.flink.streaming.window.util.TimeStampUtils;

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
public class Trade {

    private String name;

    private long amount;

    private String client;

    private Timestamp tradeTime;

    public Trade() {
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public Trade(String name, long amount, String client, String tradeTime) throws ParseException {
        this.name = name;
        this.amount = amount;
        this.client = client;
        this.tradeTime = TimeStampUtils.stringToTime(tradeTime);
    }

    public Timestamp getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(Timestamp tradeTime) {
        this.tradeTime = tradeTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                ", client='" + client + '\'' +
                ", tradeTime=" + tradeTime +
                '}';
    }
}