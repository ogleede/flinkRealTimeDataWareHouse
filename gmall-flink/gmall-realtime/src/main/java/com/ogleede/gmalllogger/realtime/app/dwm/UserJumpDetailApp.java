package com.ogleede.gmalllogger.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Ogleede
 * @Description //数据流：web/app -> nginx 发送请求-> spring boot 将数据发送到-> kafka(ods) -> flink 消费ods写入 -> kafka(dwd) -> flinkapp -> kafka(dwm)
 * //程序 : mocklog -> nginx        -> Logger.sh             ->  kafka(zk)  -> BaseLogApp     -> kafka      ->UserJumpDetailApp -> kafka
 * @create 2022-06-05-15:23
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境，与Kafka分区数保持一致

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop1:8020/gmall-flink/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //TODO 2.读取kafka dwd_page_log主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3. 将每行数据转换为JSON对象,并提取时间戳，创建watermark
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getLong("ts")));

        //TODO 4. CEP 定义模式序列（关键）
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return null == lastPageId || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                return null == lastPageId || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        // TODO 使用循环模式定义模式序列
//        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//                String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                return null == lastPageId || lastPageId.length() <= 0;
//            }
//        }).times(2)//默认是非严格近邻，如要实现严格近邻，要额外指定
//                .consecutive()
//                .within(Time.seconds(10));

        //TODO 5. CEP 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(jsonDS.keyBy(json -> json.getJSONObject("common").getString("mid")), pattern);

        //TODO 6. CEP 提取匹配上的和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("time-out") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                });

        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //TODO 7. UNION两种事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //TODO 8. 将事件写入Kafka
        unionDS.print();
        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 9. 启动任务
        env.execute("UserJumpDetailApp");
    }
}
