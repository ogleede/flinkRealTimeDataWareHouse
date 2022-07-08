package com.ogleede.gmalllogger.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {
    private static String brokers = "hadoop1:9092,hadoop2:9092,hadoop3:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer(brokers,
                topic,
                new SimpleStringSchema());
    }

    /**
     *
     * @param <T> 方法泛型要在前面也要声明
     * @return
     * @Description : 谁用这个方法的时候，肯定知道数据流的类型。再实现对应的schema就可以了
     */
    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaProducer<T>(
                default_topic,
                kafkaSerializationSchema,//这里作为参数传进来，因为调用外层方法的时候，类型已经确定了。
                properties,
                FlinkKafkaProducer.Semantic.NONE);
        //这里如果不开checkpoint，精确一次是无用的。
        //开了checkpoint，这里开启精确一次，会爆一个错误~~~~~~~~待解决
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);


        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    public static String getKafkaDDL(String sourceTopic, String groupId) {
        String ddl =
                "'connector' = 'kafka', " +
                " 'topic' = '" + sourceTopic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset' ";
        return ddl;
    }
}
