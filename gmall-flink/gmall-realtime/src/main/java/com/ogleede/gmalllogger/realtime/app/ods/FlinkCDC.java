package com.ogleede.gmalllogger.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ogleede.gmalllogger.realtime.app.function.CustomerDeserialization;
import com.ogleede.gmalllogger.realtime.common.MysqlConstant;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1开启checkpoint 并指定状态后端为FS
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/checkpoint"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(MysqlConstant.MYSQL_HOST)
                .port(MysqlConstant.MYSQL_PORT)
                .username(MysqlConstant.MYSQL_USERNAME)
                .password(MysqlConstant.MYSQL_PASSWORD)
                .databaseList(MysqlConstant.FLINKCDC_DATABASE)//监控哪个库
                //.tableList("gmall-flink.base_trademark")//监控库中的哪个表，如果不加，就是监控所有表。如果指定，指定方式为db.table
                .deserializer(new CustomerDeserialization())
//                .startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        //3.打印数据并写入kafka
//        streamSource.print();

        String sinkTopic = "ods_base_db";
        streamSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //4.启动任务
        env.execute("FlinkCDC");
    }
}
