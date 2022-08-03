package com.ogleede.gmalllogger.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ogleede.gmalllogger.realtime.app.function.CustomerDeserialization;
import com.ogleede.gmalllogger.realtime.app.function.DimSinkFunction;
import com.ogleede.gmalllogger.realtime.app.function.TableProcessFunction;
import com.ogleede.gmalllogger.realtime.bean.TableProcess;
import com.ogleede.gmalllogger.realtime.common.MysqlConstant;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * 有主流和广播流，这两个流只要谁有数据来了，都会进行处理。
 * 有可能先启动FlinkCDC时，主流数据先来，而这时广播流数据还没来，配置信息还没有，那这条数据就丢了。
 * 1.可以在TableProcess的open中存状态，写入Map中。在判断tableProcess为null时，再判断一下是否在Map中
 * 2.或者在判断tableProcess为null时，直接访问MySQL，去看一下key是否存在不
 */

/**
 * @Description:
 *  数据流：web/app->nginx->Sprintboot->MySQL->Flink   ->Kafka(ods)->Flink    ->Kafka(dwd)/Phoenix(dim)
 *  程序：  (-------mock-------------)->MySQL->FlinkCDC->Kafka(ZK)->BaseDBAPP->Kafka/Phoenix(hbase,zk,hdfs)
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //DONE 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1开启checkpoint 并指定状态后端为FS
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/checkpoint"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //DONE 2.消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //DONE 3.将每行数据转换为JSON对象并过滤(delete)   主流
        //业务数据一般无脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(line -> JSON.parseObject(line))
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出数据的操作类型
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });

        //DONE 4.使用FlinkCDC消费配置表，并处理成 广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(MysqlConstant.MYSQL_HOST)
                .port(MysqlConstant.MYSQL_PORT)
                .username(MysqlConstant.MYSQL_USERNAME)
                .password(MysqlConstant.MYSQL_PASSWORD)
                .databaseList(MysqlConstant.FLINKCDC_CONF_DATABASE)//业务库不允许在运行时建表，创建新库。
                .tableList(MysqlConstant.FLINKCDC_CONF_DATABASE + "." + MysqlConstant.FLINKCDC_CONF_TABLE)
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        //利用广播流，将主流中的每一张表发送到不同的地方去

        //table_process字段
        // sourceTable表名    type操作类型  sinkType    sinkTable          sinkColumns(HBase建表用)   pk主键(Phoenix建表用)   extend（扩展字段，说明是否做预分区等等）
        //base_trademark     insert        hbase     dim_xxx(Phoenix表名)    对主流数据进行过滤，
        //order_info         insert        kafka     dwd_xxa(主题名)          比如主流有三个字段，但是
        //order_info         update        kafka     dwd_xxb(主题名)           建表只需要两个
        //选择表名+操作类型作为主键
        //kafka会自动创建主题。而phoenix的表必须提前创建好。

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state", String.class, TableProcess.class);//key为主键(表名+操作类型），val为整行数据
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);


        //DONE 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //广播流：
            //解析数据 String -> TableProcess
            //检查HBase表是否存在，如果不存在则在Phoenix中建表
            //写入状态
        //主流：
            //获取广播的配置数据
            //过滤数据-sink columns
            //分流

        //DONE 6.分流（分成kafka和HBase）： 处理数据  广播流数据，主流数据（根据广播流数据进行处理）
        //kafka主流 Hbase侧输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag"){
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));

        //DONE 7.提取kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);


        //DONE 8.将kafka数据写入kafka主题，将HBase数据写入phoenix表
        kafka.print("kafka>>>>>>>>>>");
        hbase.print("hbase>>>>>>>>>>");

        //?的数量不确定,不能采用这种方式
//        hbase.addSink(JdbcSink.sink("upsert into t values(?,?,?,?)", )
        hbase.addSink(new DimSinkFunction());
        //kafka的数据主题不一样
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(
                        element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));

        //DONE 9.启动任务
        env.execute("BaseDBApp");
    }
}
