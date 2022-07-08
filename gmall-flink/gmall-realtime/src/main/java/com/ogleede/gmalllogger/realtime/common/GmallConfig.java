package com.ogleede.gmalllogger.realtime.common;

/**
 * @author Ogleede
 * @Description 常量类- HBase和Phoenix配置信息
 * @create 2022-06-03-14:02
 */
public class GmallConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA =
            "GMALL_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER =
            "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER =
            "jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181";
    //ClickHouse url
    public static final String CLICKHOUSE_URL=
            "jdbc:clickhouse://hadoop1:8123/default";
    //ClickHouse driver
    public static final String CLICKHOUSE_DRIVER =
            "ru.yandex.clickhouse.ClickHouseDriver";
}
