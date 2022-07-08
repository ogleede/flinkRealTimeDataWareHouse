package com.ogleede.gmalllogger.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;

import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Ogleede
 * @Description 向 ClickHouse 写入数据的时候，如果有字段数据不需要传输，可以用该注解标记
 * @Target(ElementType.FIELD) 指定作用范围
 * @Retention(RetentionPolicy.RUNTIME) 注解不仅被保存到class文件中，jvm加载class文件之后，仍然存在；
 * https://blog.csdn.net/qq_18671415/article/details/111866546
 * @create 2022-06-30-23:49
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
