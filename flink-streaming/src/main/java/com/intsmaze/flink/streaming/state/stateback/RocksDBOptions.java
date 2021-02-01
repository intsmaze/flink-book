//package com.intsmaze.flink.streaming.state.stateback;
//
//import org.apache.flink.contrib.streaming.state.OptionsFactory;
//import org.rocksdb.BlockBasedTableConfig;
//import org.rocksdb.ColumnFamilyOptions;
//import org.rocksdb.DBOptions;
//
///**
// * github地址: https://github.com/intsmaze
// * 博客地址：https://www.cnblogs.com/intsmaze/
// * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
// *
// * @auther: intsmaze(刘洋)
// * @date: 2020/10/15 18:33
// */
//public class RocksDBOptions implements OptionsFactory {
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Override
//    public DBOptions createDBOptions(DBOptions currentOptions) {
//        return currentOptions.setIncreaseParallelism(5)
//                .setUseFsync(false);
//    }
//
//    /**
//     * github地址: https://github.com/intsmaze
//     * 博客地址：https://www.cnblogs.com/intsmaze/
//     * 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
//     *
//     * @auther: intsmaze(刘洋)
//     * @date: 2020/10/15 18:33
//     */
//    @Override
//    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
//        return currentOptions.setTableFormatConfig(
//                new BlockBasedTableConfig()
//                        .setBlockCacheSize(256 * 1024 * 1024)
//                        .setBlockSize(128 * 1024));
//    }
//}
