package com.ogleede.gmalllogger.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.app.function.AbstractDimAsyncFunction;
import com.ogleede.gmalllogger.realtime.bean.OrderDetail;
import com.ogleede.gmalllogger.realtime.bean.OrderInfo;
import com.ogleede.gmalllogger.realtime.bean.OrderWide;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

//数据流： web/app->nginx->SpringBoot->mysql->FlinkApp->Kafka(ods)->FlinkApp->
// Kafka/HBase(dwd/dim)        ->FlinkApp      ->Kafka(dwm)
//程序：   mockDb                    ->mysql->FlinkApp->Kafka(zk)-> BaseDBApp->
//kafka/phoenix(zk,hdfs,hbase) -> OrderWideApp -> kafka
/**
 * @author Ogleede
 * @Description 订单宽表
 * @create 2022-06-07-20:43
 */
public class OrderWideApp {
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

        //TODO 1. 读取Kafka主题的数据 dwd_order_info dwd_order_detail,转换为JavaBean对象&提取时间戳生成WM

        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_test";

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                    /**
                     * sdf 内部的calendar是全局变量，存在线程不安全现象。所以不写在外面
                     */
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts()));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_ts()));

        //TODO 2. 双流join
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDimDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))//生产环境中，这里的时间给的是最大延迟时间，不会丢数据
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        orderWideWithoutDimDS.print("orderWideWithoutDimDS>>>>>>>>");
        //双流join的测试结果，应该是和被join表的数量相等。
        //当一对N时，生成N项
        //当多对N时，多有分区（keyBy），N也有分区（keyBy），最后的结果数量是，多的分区分别占有N的每个key。结果相加还是N。
        //上述前提是时间生成较短，全部被interval抓住的情况。

        //TODO 3. 关联维表*** （维度信息在Phoenix中，要用map方法去查询）
//        orderWideWithoutDimDS.map(orderWide -> {
//            //关联用户维度
//
//            //根据user_id查询Phoenix用户信息
//
//            //将用户信息补充至orderWide
//
//            //返回结果
//        });

        //3.1 关联用户维度
        /**
         * 访问zk的超时时间是60s，这里的超时时间至少是60s
         */
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideWithoutDimDS,
                new AbstractDimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setUser_gender(dimInfo.getString("GENDER"));

                        /**
                         * 改进：生日转化为年龄
                         */
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long curTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();
                        Long age = (curTs - ts) / (1000 * 60 * 60 * 24 * 365l);
                        input.setUser_age(age.intValue());

                    }
                },
                60,
                TimeUnit.SECONDS);

        //打印测试
//        orderWideWithUserDS.print("orderWideWithUserDS");
        //3.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new AbstractDimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setProvince_name(dimInfo.getString("NAME"));
                        input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        input.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60, TimeUnit.SECONDS);
        /**
         * 这里要看一下数据仓库的模型，星形模型、雪花模型
         */
        //3.3 关联SKU维度,sku一定要在下面三个之前
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithProvinceDS, new AbstractDimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException{
                                orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                                orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                                orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSku_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //3.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSkuDS, new AbstractDimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException{
                                orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //3.5 关联TM维度（品牌维度）
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDS, new AbstractDimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException{
                                orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //3.6 关联Category维度（品类维度）
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS =
                AsyncDataStream.unorderedWait(
                        orderWideWithTmDS, new AbstractDimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //打印测试
        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>");

        //TODO 4. 将数据写入Kafka
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        env.execute("OrderWideApp");
    }
}
