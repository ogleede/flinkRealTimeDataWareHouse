package com.ogleede.gmalllogger.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.bean.VisitorStats;
import com.ogleede.gmalllogger.realtime.utils.ClickHouseUtil;
import com.ogleede.gmalllogger.realtime.utils.DateTimeUtil;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

//数据流：web/app -> nginx 发送请求-> spring boot 将数据发送到-> kafka(ods) -> flink 消费ods写入 -> kafka(dwd) ->FlinkApp ->kafka(dwm)
// -> FlinkApp -> ClickHouse
//程序 : mocklog -> nginx        -> Logger.sh             ->  kafka(zk)  -> BaseLogApp     -> kafka    -> uv/uj -> kafka
// -> VisitorStatsApp -> ClickHouse

/**
 * uj数据wm是1s,同一条数据被两个不同的流消费，result流输出时，uj流要多10s才输出，result流流的窗口已经关闭，所以uj一直为0
 * 这里要提高result流的窗口WM延迟时间，1s -> 11s
 * uj因为要等待时间才能统计，所以和uj相关的指标要保证幂等性(即使用事件时间)，没办法保证特别高的实时性
 */

/**
 * @author Ogleede
 * @Description dws层访客主题宽表
 * @create 2022-06-29-23:43
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO:这里如果不注释掉会报错，待解决,问题出现在H状态后端这里要访问HDFS，但是权限不够。之后集群运行时再解决。
        //1.1开启checkpoint 并指定状态后端为FS
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/checkpoint"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //TODO 1 读取kafka数据，创建流。分别是kafka中的pv、uv、跳转明细主题
        String groupId = "visitor_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        //TODO 2 将每个流处理成相同的数据类型
        //处理UV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));
        });

        //处理UJ数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUJDS = ujDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));
        });

        //**处理PV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPVDS = pvDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            //提取公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            //获取上一跳页面ID
            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");

            Long sv = 0L;//进入页面数

            if (last_page_id == null || last_page_id.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, page.getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //TODO 3 Union几个流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUJDS, visitorStatsWithPVDS);

        //TODO 4 提取时间戳生成WM
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWM = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        //TODO 5 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWM.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(
                        value.getAr(),
                        value.getCh(),
                        value.getIs_new(),
                        value.getVc()
                );
            }
        });

        //TODO 6 开窗聚合，10s滚动窗口(大屏刷新间隔是10s)
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        /*
         * 要拿到窗口信息，不需用process，window里面的apply也有窗口信息
         * 如果只用reduce做增量聚合是不能利用窗口信息全量聚合的
         * reduce里面可以再传入一个windowFunction，增量聚合时走reduceFunction，最后窗口输出时，将聚合结果给到windowFunction
         * 此时全量聚合的迭代器中只有一条数据，即触发窗口时，增量聚合给的数据
         */
        SingleOutputStreamOperator<VisitorStats> result = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                /*
                 * 这里选择的是滚动窗口，可以在value1中set新值，返回value1
                 * 如果是滑动窗口，只能new新对象赋值，因为当前对象会被其他窗口利用
                 * 正因为用的是滚动窗口，所以ts字段用的value1的
                 */
//                return new VisitorStats(value1.getStt(), value1.getEdt(),
//                        value1.getVc(),value1.getCh(),value1.getAr(),value1.getIs_new(),
//                        value1.getUv_ct() + value2.getUv_ct(),
//                        value1.getPv_ct() + value2.getPv_ct(),
//                        value1.getSv_ct() + value2.getSv_ct(),
//                        value1.getUj_ct() + value2.getUj_ct(),
//                        value1.getDur_sum() + value2.getDur_sum(),
//                        value1.getTs());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                long start = window.getStart(), end = window.getEnd();

                VisitorStats visitorStats = input.iterator().next();
                //补充窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));

                out.collect(visitorStats);
            }
        });

        //TODO 7 将数据写入ClickHouse
        //打印测试
        result.print(">>>>>>>>>>>>>>>");
        /*
         * Phoenix 也可以用JDBC方式，但是每个表不一样，表的字段不同，不好指定，所以不用JDBC方式
         * 在向ClickHouse时，可以，result相当于一张表。
         * 当Bean字段顺序和表字段顺序一致时，sql可以不写字段顺序
         * 如果写的话就是："insert into visitor_stats(id) values(1)"
         */
        result.addSink(ClickHouseUtil.getSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute("VisitorStatsApp");
    }
}
