package com.intsmaze.flink.streaming.state.key;

import java.io.Serializable;

     /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
public class AverageAccumulator implements Serializable {

    private long count=0;

    private double sum=0.0;

    public Double getLocalValue() {
        if (this.count == 0) {
            return 0.0;
        }
        return this.sum / this.count;
    }

    public void add(int value) {
        this.count++;
        this.sum += value;
    }

    public void add(long count,double sum) {
        this.count+=count;
        this.sum += sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }
}
