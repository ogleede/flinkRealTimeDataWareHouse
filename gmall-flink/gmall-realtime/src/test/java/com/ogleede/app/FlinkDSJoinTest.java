package com.ogleede.app;

import com.ogleede.bean.Bean1;
import com.ogleede.bean.Bean2;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Ogleede
 * @Description
 * @create 2022-06-07-20:05
 */
public class FlinkDSJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取两个端口数据创建流并提取时间戳生成WaterMark
        //nc -lk 8888
        SingleOutputStreamOperator<Bean1> stream1 = env.socketTextStream("hadoop1", 8888)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean1(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L));

        SingleOutputStreamOperator<Bean2> stream2 = env.socketTextStream("hadoop1", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Bean2(fields[0], fields[1], Long.parseLong(fields[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L));

        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> joinDS = stream1.keyBy(Bean1::getId)
                .intervalJoin(stream2.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
//                .lowerBoundExclusive()//加了就是左开区间
//                .upperBoundExclusive()//加了就是右开区间
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {

                    @Override
                    public void processElement(Bean1 left, Bean2 right, ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>.Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        joinDS.print();
        env.execute();
    }
}
