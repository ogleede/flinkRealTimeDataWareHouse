package com.ogleede.gmalllogger.realtime.app.dws;

import com.ogleede.gmalllogger.realtime.bean.ProvinceStats;
import com.ogleede.gmalllogger.realtime.utils.ClickHouseUtil;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Ogleede
 * @Description 地区主题主要是反映各个地区的销售情况。
 * 从业务逻辑上讲，地区主题主要做一次轻度聚合然后保存。
 * 地区主题有三个指标：pv，uv，下单单数，下单总金额。
 * 其中pv：可以从page_log求得
 * uv：可从page_log过滤去重求得
 * 下单信息：可从订单宽表求得
 * pv,uv 在访客主题中有地区维度，后续可以从订单主题中求得。地区主题重点求下单信息。
 * @create 2022-07-03-20:39
 */
//数据流： web/app->nginx->SpringBoot->mysql->FlinkApp->Kafka(ods)->FlinkApp->
// Kafka/HBase(dwd/dim)        ->FlinkApp      ->Kafka(dwm) -> FlinkAPP -> ClickHouse
//程序：   mockDb                    ->mysql->FlinkApp->Kafka(zk)-> BaseDBApp->
//kafka/phoenix(zk,hdfs,hbase) -> OrderWideApp -> kafka    -> ProvinceStatsSQLApp -> ClickHouse

/**
 * 由于mockDB造数据是一次性造完，造完数据还没达到窗口的关闭时间，所以要控制间隔在窗口间隔以上，造两次数据。
 * 由于这里还没做CheckPoint，间隔时间过长时，之前的数据没有消费，仍需要造两次
 */

public class ProvinceStatsSQLApp {
    public static void main(String[] args) throws Exception {

        // TODO 1 获取执行环境、获取表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1开启checkpoint 并指定状态后端为FS
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/checkpoint"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        // TODO 2 使用DDL创建表 提取时间戳，生成WM
        String sourceTopic = "dwm_order_wide";
        String groupId = "province_stats";

        tableEnv.executeSql(
                "CREATE TABLE order_wide ( " +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `order_id` BIGINT, " +
                "  `split_total_amount` DECIMAL, " +
                "  `create_time` STRING, " +
                "  `rt` as TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND )" +
                " with ( " +
                MyKafkaUtil.getKafkaDDL(sourceTopic, groupId) + " )");


        // TODO 3 查询数据  分组、开窗、聚合
        Table table = tableEnv.sqlQuery(
                "select " +
                        "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                        "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                        "province_id, " +
                        "province_name, " +
                        "province_area_code, " +
                        "province_iso_code, " +
                        "province_3166_2_code, " +
                        "COUNT(DISTINCT order_id) order_count, " +
                        "sum(split_total_amount) order_amount, " +
                        "UNIX_TIMESTAMP()*1000 ts " +
                    "from " +
                        "order_wide " +
                    "group by " +
                        "TUMBLE(rt, INTERVAL '10' SECOND), " +
                        "province_id, " +
                        "province_name, " +
                        "province_area_code, " +
                        "province_iso_code, " +
                        "province_3166_2_code");

        // TODO 4 将动态表转换为流
        /**
         * 仅追加流：
         * 撤回流 ：
         */
        DataStream<ProvinceStats> productStatsDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        // TODO 5 写入ClickHouse
        /**
         * 如果是Flink官方支持的数据库，也可以直接把目标数据表定义为动态表，用insert into写入。
         * 而ClickHouse目前官方没有支持的jdbc连接器（目前支持Mysql、 PostgreSQL、Derby）。
         * 也可以制作自定义 sink，实现官方不支持的连接器。但是比较繁琐。
         * 转换为流写入，可以利用已有的ClickHouseUtil
         */
        productStatsDS.print(">>>>>>");

        /**
         * ClickHouseUtil.getSink() 是按位置插入的，字段名称不同没关系
         * 如果不按位置插入，需要指定表的字段名称
         */
        productStatsDS.addSink(ClickHouseUtil.getSink(
                "insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute("ProvinceStatsSQLApp");
    }
}
