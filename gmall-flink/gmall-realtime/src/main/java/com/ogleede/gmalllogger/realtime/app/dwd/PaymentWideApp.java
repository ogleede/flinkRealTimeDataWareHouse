package com.ogleede.gmalllogger.realtime.app.dwd;

/**
 * 有两种方式
 * 1.将订单宽表输出到HBase上，在支付宽表计算时查询HBase，相当于把订单宽表作为维度管理
 * 2.用流的方式接收订单宽表，使用双流join合并。因为订单与支付间有一定的时间差，
 *      必须用intervalJoin来管理流的状态时间，保证当支付到达时订单宽表还保存在状态中。
 * 1.劣势在于，HBase一般保存是永久的(可以设置TTL)，而且订单宽表已经输出到Kafka中了，没必要再写入一次
 * 2.劣势在于，支付和订单之间的时间差可能很长，要保留一个很长时间的状态，不过15min等时间是可以接收的。
 */

import com.alibaba.fastjson.JSON;
import com.ogleede.gmalllogger.realtime.bean.OrderWide;
import com.ogleede.gmalllogger.realtime.bean.PaymentInfo;
import com.ogleede.gmalllogger.realtime.bean.PaymentWide;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 测试用SQL：
 * SELECT *
 * from payment_info p
 * join order_detail o
 * on p.order_id = o.order_id;
 */

//数据流： web/app->nginx->SpringBoot->mysql->FlinkApp->Kafka(ods)->FlinkApp->
// Kafka/HBase(dwd/dim)        ->FlinkApp      ->Kafka(dwd)
//程序：   mockDb                    ->mysql->FlinkApp->Kafka(zk)-> BaseDBApp->
//kafka/phoenix(zk,hdfs,hbase) -> OrderWideApp -> kafka -> PaymentWideAPP -> kafka
/**
 * @author Ogleede
 * @Description
 * @create 2022-06-29-0:18
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //DONE 1读取Kafka主题的数据，创建流，并转换成JavaBean对象，提取时间戳，生成WM
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwd_order_wide";
        String paymentWideSinkTopic = "dwd_payment_wide";

        SingleOutputStreamOperator<OrderWide> orderWideDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId))
                .map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        //DONE 2 双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.minutes(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        //DONE 3 将数据写入Kafka
        paymentWideDS.print(">>>>>>>");
        paymentWideDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        env.execute("PaymentWideAPP");
    }
}
