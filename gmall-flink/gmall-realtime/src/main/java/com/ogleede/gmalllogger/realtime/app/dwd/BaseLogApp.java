package com.ogleede.gmalllogger.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app -> nginx 发送请求-> spring boot 将数据发送到-> kafka(ods) -> flink 消费ods写入 -> kafka(dwd)
//程序 : mocklog -> nginx        -> Logger.sh             ->  kafka(zk)  -> BaseLogApp     -> kafka

public class BaseLogApp {
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


        //DONE 2.消费 ods_base_log 主题数据 创建流
        String sourceTopic = "ods_base_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //DONE 3.将每行数据转换为JSON对象
//        kafkaDS.map(line -> {//如果用map方法，无法处理脏数据，比如字符串最后少了个大括号
//            return JSON.parseObject(line);
//        });
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //发生异常，将数据写入侧输出流
                    context.output(dirtyOutputTag, value);
                }
            }
        });

        //打印脏数据
        jsonObjDS.getSideOutput(dirtyOutputTag).print("dirty>>>>>>>>>>>");

        //DONE 4.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {//对状态进行初始化
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //获取数据中的"is_new"标记
                        String isNew = value.getJSONObject("common").getString("is_new");

                        //判断isNew标记是否为"1"
                        if ("1".equals(isNew)) {
                            //获取状态数据
                            String state = valueState.value();
                            if (null != state) {
                                //修改isNew标记
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                valueState.update("1");
                            }
                        }
                        return value;
                    }
                });

        //DONE 5.分流 测输出流  页面：主流   启动：侧输出流  曝光：侧输出流
        //侧输出流->只能用process
        //分流写入kafka

        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //获取启动日志字段
                String start = value.getString("start");
                if (null != start && start.length() > 0) {
                    //将数据写入启动日志侧输出流
                    context.output(startOutputTag, value.toJSONString());
                } else {
                    //将数据写入页面日志主流
                    collector.collect((value.toJSONString()));
                    //取出数据中的曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (null != displays && displays.size() > 0) {
                        //获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            //添加页面id
                            display.put("page_id", pageId);
                            //将输出写出到曝光侧输出流
                            context.output(displayOutputTag, display.toJSONString());
                        }
                    }
                }
            }
        });


        //DONE 6.提取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);

        //DONE 7.将三个流进行打印并输入到对应的kafka主题中
        startDS.print("start>>>>>>>>>>>>>");
        pageDS.print("page>>>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>>");

        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //bin/kafka-console-producer.sh --broker-list hadoop1:9092 --topic ods_base_log
        //bin/kafka-console-consumer.sh --bootstrap-server hadoop1:9092 --topic dwd_start_log
        //bin/kafka-console-consumer.sh --bootstrap-server hadoop1:9092 --topic dwd_page_log
        //bin/kafka-console-consumer.sh --bootstrap-server hadoop1:9092 --topic dwd_display_log

        //DONE 8.启动任务
        env.execute("BaseLogApp");
    }
}
