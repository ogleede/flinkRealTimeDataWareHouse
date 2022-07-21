package com.ogleede.gmalllogger.realtime.utils;

import com.ogleede.gmalllogger.realtime.bean.TransientSink;
import com.ogleede.gmalllogger.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Ogleede
 * @Description
 * @create 2022-06-30-23:09
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.sink(
                sql,

                new JdbcStatementBuilder<T>() {
                    /*
                     * 把 t 的值取出来，给preparedStatement的 ? 赋值
                     */
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        /*
                          getDeclaredFields() 返回全字段
                          getFields()         返回public字段
                         */
                        try {
                            Field[] fields = t.getClass().getDeclaredFields();
                            int idx = 0;
                            for (int i = 0; i < fields.length; i++) {
                                //获取字段
                                Field field = fields[i];
                                //设置私有属性可访问
                                field.setAccessible(true);

                                //获取字段上的注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation != null) {
                                    /**
                                     * 存在该注解,表明该字段是建表辅助字段，不写入sql
                                     */
                                    continue;
                                }

                                //获取值
                                Object value = field.get(t);

                                //给预编译SQL赋值
                                preparedStatement.setObject(++idx, value);
                            }

                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },

                new JdbcExecutionOptions.Builder()
                        .withBatchIntervalMs(2)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
