package com.intsmaze.flink.table;

import com.intsmaze.flink.table.bean.ClickBean;
import com.intsmaze.flink.table.bean.Order;
import com.intsmaze.flink.table.bean.Person;
import com.intsmaze.flink.table.bean.RateBean;

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
        clickList.add(new ClickBean(3, "张三", "./intsmaze", "2019-07-28 12:08:08"));
        clickList.add(new ClickBean(4, "张三", "./sql", "2019-07-28 12:30:00"));
        clickList.add(new ClickBean(5, "李四", "./intsmaze", "2019-07-28 13:01:00"));
        clickList.add(new ClickBean(6, "王五", "./flink", "2019-07-28 13:20:00"));
        clickList.add(new ClickBean(7, "王五", "./sql", "2019-07-28 13:30:00"));
        clickList.add(new ClickBean(8, "张三", "./intsmaze", "2019-07-28 14:10:00"));
        clickList.add(new ClickBean(9, "王五", "./flink", "2019-07-28 14:20:00"));
        clickList.add(new ClickBean(10, "李四", "./intsmaze", "2019-07-28 14:30:00"));
        clickList.add(new ClickBean(11, "李四", "./sql", "2019-07-28 14:40:00"));
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
    public static List<Person> getPersonData() {
        List<Person> personList = new ArrayList();
        personList.add(new Person("张三", 38, "上海"));
        personList.add(new Person("李四", 45, "深圳"));
        personList.add(new Person("赵六", 18, "天津"));
        return personList;
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static List<RateBean> getRateData() throws ParseException {
        List<RateBean> rateList = new ArrayList();
        rateList.add(new RateBean(1, "US Dollar", 102, "2019-07-28 09:00:00"));
        rateList.add(new RateBean(2, "Euro", 114, "2019-07-28 09:20:00"));
        rateList.add(new RateBean(3, "Yen", 1, "2019-07-28 09:30:00"));
        rateList.add(new RateBean(4, "Euro", 116, "2019-07-28 10:45:00"));
        rateList.add(new RateBean(5, "Euro", 119, "2019-07-28 11:15:00"));
        rateList.add(new RateBean(6, "Pounds", 108, "2019-07-28 11:49:00"));
        return rateList;
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static List<Order> getOrderData() throws ParseException {
        List<Order> orderList = new ArrayList();
        orderList.add(new Order(1, "US Dollar", "2019-07-28 09:00:00", 2));
        orderList.add(new Order(2, "Euro", "2019-07-28 09:00:00", 10));
        orderList.add(new Order(3, "Yen", "2019-07-28 09:40:00", 30));
        orderList.add(new Order(4, "Euro", "2019-07-28 11:40:00", 30));
        return orderList;
    }

}
