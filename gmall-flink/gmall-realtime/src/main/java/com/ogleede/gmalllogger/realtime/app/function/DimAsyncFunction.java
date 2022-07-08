package com.ogleede.gmalllogger.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.common.GmallConfig;
import com.ogleede.gmalllogger.realtime.utils.DimUtil;
import com.ogleede.gmalllogger.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Ogleede
 * @Description 异步的方法去给维表查询，用线程池实现
 * @create 2022-06-27-20:57
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T>{
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

//    /**
//     *  模板设计模式
//     * @param input 但从input可能是OrderWide来看，就有多种维度，id类型不同。
//     *              况且是泛型方法，无法确定具体的id是什么，需要写成抽象方法，调用者去实现。
//     *              当外面去实现这个方法时，会用确定的类型，比如OrderWide
//     * @return
//     */
//    public abstract String getKey(T input);
//
//    /**
//     * 和上面同理，两者信息不知道具体类型，给调用者去实现
//     * @param input
//     * @param dimInfo
//     */
//    public abstract void join(T input, JSONObject dimInfo) throws ParseException;

    /**
     * run方法中的tableName不在input中，需要通过构造方法传入。
     * id在input中解析，但是如果需要关联的信息不确定，id在OrderWide中的名字是不同的
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {


            @Override
            public void run() {
                try {
                    //获取查询主键
                    String id = getKey(input);
                    //查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    //补充维度信息
                    if(dimInfo != null) {
                        join(input, dimInfo);
                    }
                    //数据输出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }



    /**
     * 异步IO超时
     */
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut" + input);
    }
}
