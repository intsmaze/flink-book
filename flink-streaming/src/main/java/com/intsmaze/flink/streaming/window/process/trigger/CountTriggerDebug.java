package com.intsmaze.flink.streaming.window.process.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;


/**
 * github地址: https://github.com/intsmaze
 * 博客地址：https://www.cnblogs.com/intsmaze/
 * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: intsmaze(刘洋)
 * @date: 2020/10/15 18:33
 */
public class CountTriggerDebug<W extends Window> extends Trigger<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new CountTriggerDebug.Sum(), LongSerializer.INSTANCE);

    private CountTriggerDebug(long maxCount) {
        this.maxCount = maxCount;
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
    public TriggerResult onElement(Object element, long timestamp, W window, Trigger.TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
        count.add(1L);
        if (count.get() >= maxCount) {
            System.out.println("触发器触发窗口函数对该窗口计算,同时清除该窗口的计数状态,--" + count.get());
            count.clear();
//            return TriggerResult.FIRE;
            return TriggerResult.FIRE_AND_PURGE;
        }
        System.out.println("触发器仅对该窗口的计数状态进行加一操作--" + count.get());
        return TriggerResult.CONTINUE;
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
    public TriggerResult onEventTime(long time, W window, Trigger.TriggerContext ctx) {
        System.out.println("触发器调用 onEventTime 方法");
        return TriggerResult.CONTINUE;
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
    public TriggerResult onProcessingTime(long time, W window, Trigger.TriggerContext ctx) {
        System.out.println("触发器调用 onProcessingTime 方法");
        return TriggerResult.CONTINUE;
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
    public void clear(W window, Trigger.TriggerContext ctx) {
        System.out.println("触发器调用 clear 方法");
        ctx.getPartitionedState(stateDesc).clear();
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
    public boolean canMerge() {
        return true;
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
    public void onMerge(W window, Trigger.OnMergeContext ctx) {
        System.out.println("触发器调用 onMerge 方法");
        ctx.mergePartitionedState(stateDesc);
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
    public String toString() {
        return "CountTrigger(" + maxCount + ")";
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    public static <W extends Window> CountTriggerDebug<W> of(long maxCount) {
        return new CountTriggerDebug<>(maxCount);
    }

    /**
     * github地址: https://github.com/intsmaze
     * 博客地址：https://www.cnblogs.com/intsmaze/
     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: intsmaze(刘洋)
     * @date: 2020/10/15 18:33
     */
    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        /**
         * github地址: https://github.com/intsmaze
         * 博客地址：https://www.cnblogs.com/intsmaze/
         * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
         *
         * @auther: intsmaze(刘洋)
         * @date: 2020/10/15 18:33
         */
        @Override
        public Long reduce(Long value1, Long value2) {
            System.out.println("触发器调用  reduce方法 " + value1 + ":" + value2);
            return value1 + value2;
        }

    }
}

