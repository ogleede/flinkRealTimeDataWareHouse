package com.ogleede.gmalllogger.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.app.function.AbstractDimAsyncFunction;
import com.ogleede.gmalllogger.realtime.bean.OrderWide;
import com.ogleede.gmalllogger.realtime.bean.PaymentWide;
import com.ogleede.gmalllogger.realtime.bean.ProductStats;
import com.ogleede.gmalllogger.realtime.common.GmallConstant;
import com.ogleede.gmalllogger.realtime.utils.ClickHouseUtil;
import com.ogleede.gmalllogger.realtime.utils.DateTimeUtil;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Ogleede
 * @Description * 8 个指标 , 来自7个流
 * * 点击
 * * 曝光
 * * 收藏
 * * 加入购物车
 * * 下单
 * * 支付
 * * 退款
 * * 评价
 * @create 2022-07-02-16:21
 */
//数据流：
// app/web->nginx->SpringBoot->kafka(ods)->BaseLogApp->kafka(dwd)->ProductStatsApp->ClickHouse

// app/web->nginx->SpringBoot->MySQL->FlinkCDC->kafka(ods)->BaseDBApp->kafka(dwd)/phoenix(dim)
// ->OrderWideApp/PaymentWideApp->kafka(dwd)->ProductStatsApp->ClickHouse
// 程序：省略FlinkApp，除了pv/uv/VisitorStatsApp都开
// mock->nginx->logger.sh->kafka(zk)/Phoenix(/zk/HDFS/HBase)->ClickHouse

/**
 * 运行时，要改脚本中的时间，要保证行为数据和业务数据时间一致
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1开启checkpoint 并指定状态后端为FS
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/checkpoint"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        // DONE 1 读取Kafka 7个主题 的数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwd_order_wide";
        String paymentWideSourceTopic = "dwd_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //pv流 -> 点击.曝光
        DataStreamSource<String> pvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        //favor流 -> 收藏
        DataStreamSource<String> favorDS = env.addSource(MyKafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId));
        //cart流 -> 加入购物车
        DataStreamSource<String> cartDS = env.addSource(MyKafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId));
        //order流 -> 下单
        DataStreamSource<String> orderDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        //pay流 -> 支付
        DataStreamSource<String> payDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId));
        //refund流 -> 退款
        DataStreamSource<String> refundDS = env.addSource(MyKafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId));
        //comment流 -> 评价
        DataStreamSource<String> commentDS = env.addSource(MyKafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId));

        // DONE 2 将7个流统一数据格式
        /**
         * 点击和曝光数据来自于dwd层，日志
         * 读进pv一条数据，写出多条(点击+曝光)。用flatMap
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithClickAndDisplayDS = pvDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);
                //取出page信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                Long ts = jsonObject.getLong("ts");

                /**
                 * 点击数据要判断是否进入商品详情页，这里由good_detail判定
                 * 还要做一个过滤，有可能点击的详情并不是sku，而是广告之类的无用信息
                 * 从日志获取
                 */
                if ("good_detail".equals(pageId) && "sku_id".equals(page.getString("item_type"))) {
                    out.collect(ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                /**
                 * 尝试取出曝光数据,这里可以单独写出去判断曝光数据
                 */
                JSONArray displays = jsonObject.getJSONArray("display");
                if (null != displays && !displays.isEmpty()) {
                    for (int i = 0; i < displays.size(); i++) {
                        //取出单挑曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        //判断曝光的是否是商品
                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        /**
         * 收藏信息来自于dwd层，数据库的favor_info表
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        /**
         * 加入购物车信息来自于dwd层，数据库的cart_info表
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        /**
         * 下单信息来自于dwm层,OrderWide
         * order_amount 查看的是单个商品的价格，orderWide里面的total_amount是多个商品的总金额
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            //要对订单号去重
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getOrder_price())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        /**
         * 支付信息来自于dwm层, PaymentWide
         * 支付宽表也和订单宽表做了join，payment_amount不能用total_amount
         * total_amount是订单的总金额，现在要的是商品的总金额
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithPaymentDS = payDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getOrder_price())
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        /**
         * 退款信息来自于dwd层,数据库中的order_refund_info
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        /**
         * 评价信息来自于dwd层，数据库comment_info表
         * 区分评价信息的好评与差评等信息，需要查询数据库的字典表base_dic
         * 将字典表做成常量类
         */
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            //取出评价数据
            String appraise = jsonObject.getString("appraise");
            long good = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                good = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(good)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // DONE 3 Union 7个流
        DataStream<ProductStats> unionDS = productStatsWithClickAndDisplayDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPaymentDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS
        );

        // DONE 4 提取时间戳 生成WM
        SingleOutputStreamOperator<ProductStats> productStatsWithWMDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(
                        Duration.ofSeconds(2))
                .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        // DONE 5 分组开窗聚合 按sku_id分组，10s滚窗，结合增量聚合(累加值)和全量聚合(提取窗口信息)
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWMDS
                .keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    /**
                     * 其他字段增量聚合
                     * 订单数，要去重，要合并两个HashSet
                     * 最后要输出HashSet的长度，这一步可以放在最后全量聚合做
                     */
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));
                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                        return stats1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        //取出数据
                        ProductStats productStats = input.iterator().next();

                        //补充数据
                        //设置窗口时间
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));
                        //设置订单数量
                        productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                        productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                        productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                        //将数据写出
                        out.collect(productStats);
                    }
                });

        // DONE 6 关联维度信息
        // 6.1 关联sku维度,要先关联sku维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new AbstractDimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    /**
                     * 关联Phoenix中的字段，可以从数据库的广播流table_process中的建表字段看到信息
                     */
                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                    }
                },
                60L,
                TimeUnit.SECONDS);

        // 6.2 关联spu维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new AbstractDimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getSpu_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                },
                60L,
                TimeUnit.SECONDS);

        // 6.3 关联TradeMark维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(
                productStatsWithSpuDS,
                new AbstractDimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getTm_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setTm_name(dimInfo.getString("TM_NAME"));
                    }
                },
                60L,
                TimeUnit.SECONDS);

        // 6.4 关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(
                productStatsWithTmDS,
                new AbstractDimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getTm_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setCategory3_name(dimInfo.getString("NAME"));
                    }
                },
                60L,
                TimeUnit.SECONDS);

        // DONE 7 将数据写入ClickHouse
        // 25个字段
        productStatsWithCategory3DS.addSink(ClickHouseUtil.getSink(
                "insert into table product_stats " +
                    "values(" +
                    "?,?,?,?,?," +
                    "?,?,?,?,?," +
                    "?,?,?,?,?," +
                    "?,?,?,?,?," +
                    "?,?,?,?,?)"
        ));

        env.execute("ProductStatsApp");

    }
}
