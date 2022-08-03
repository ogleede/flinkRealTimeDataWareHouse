package com.ogleede.gmalllogger.realtime.app.dws;

import com.ogleede.gmalllogger.realtime.app.function.SplitFunction;
import com.ogleede.gmalllogger.realtime.bean.KeywordStats;
import com.ogleede.gmalllogger.realtime.utils.ClickHouseUtil;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Ogleede
 * @Description
 * @create 2022-07-07-22:22
 */
//数据流：web/app -> nginx 发送请求-> spring boot 将数据发送到-> kafka(ods) -> flink 消费ods写入 -> kafka(dwd) -> flinkApp -> ClickHouse
//程序 : mocklog -> nginx        -> Logger.sh             ->  kafka(zk)  -> BaseLogApp     -> kafka    ->KeywordStatsApp -> CH

public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {

        //DONE 1 获取执行环境和表执行环境
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

        //DONE 2 使用DDL方式读取kafka 创建表
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic ="dwd_page_log";

        /**
         * 时间戳需要Date类型，用FROM_UNIXTIME函数将ts转换为Date
         */
        tableEnv.executeSql("create table page_view( " +
                "  `common` Map<STRING,STRING>, " +
                "  `page` Map<STRING,STRING>, " +
                "  `ts` BIGINT, " +
                "  `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND " +
                " ) with ( "
                + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");

        //DONE 3 过滤数据 根据page_log 中的last_page_id中，上一次页面为search && 搜索词 is not null
        Table fullWordTable = tableEnv.sqlQuery(
                "select  " +
                        "  page['item'] full_word, " +
                        "  rt " +
                        " from " +
                        "  page_view " +
                        " where " +
                        "  page['last_page_id']='search' and page['item'] is not null");


        //DONE 4 注册UDTF 进行分词处理
        tableEnv.createTemporarySystemFunction("split_words", SplitFunction.class);

        /**
         * 对fullWordTable创建视图
         * 或者直接在拼串中拼出原表
         */
        Table wordTable = tableEnv.sqlQuery(
                "select  " +
                        " word, rt " +
                    " from   " +
                        fullWordTable + ", LATERAL TABLE(split_words(full_word)) "
        );

        //DONE 5 分组开窗聚合
        Table resultTable = tableEnv.sqlQuery(
                "select  " +
                        " 'search' source, " +
                        " DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                        " DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,  " +
                        " word keyword, " +
                        " count(*) ct, " +
                        " UNIX_TIMESTAMP()*1000 ts " +
                        "from " + wordTable +
                        " group by  " +
                        " word, " +
                        " TUMBLE(rt, INTERVAL '10' SECOND)");

        //DONE 6 将动态表转换为流
        /**
         * 把表转换为流，并生成entity，这其中的对应是怎么来的呢？
         * 是按名称？应该是按名称来的，因为位置是乱的那么名称都要对应，不能有差别
         */
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        //DONE 7 将数据写入ClickHouse
        keywordStatsDataStream.print(">>>>>>");

        /**
         * 名字按sink表的名字写，顺序按照entity的顺序写
         */
        keywordStatsDataStream.addSink(ClickHouseUtil.getSink(
                "insert into keyword_stats(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        env.execute("KeywordStatsApp");
    }
}
