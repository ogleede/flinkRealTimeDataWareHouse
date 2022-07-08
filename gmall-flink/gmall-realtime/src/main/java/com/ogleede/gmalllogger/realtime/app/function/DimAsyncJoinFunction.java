package com.ogleede.gmalllogger.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author Ogleede
 * @Description
 * @create 2022-06-28-22:27
 */
public interface DimAsyncJoinFunction<T> {
    //    /**
//     *  模板设计模式
//     * @param input 但从input可能是OrderWide来看，就有多种维度，id类型不同。
//     *              况且是泛型方法，无法确定具体的id是什么，需要写成抽象方法，调用者去实现。
//     *              当外面去实现这个方法时，会用确定的类型，比如OrderWide
//     * @return
//     */
    String getKey(T input);
//
//    /**
//     * 和上面同理，两者信息不知道具体类型，给调用者去实现
//     * @param input
//     * @param dimInfo
//     */
    void join(T input, JSONObject dimInfo) throws ParseException;
}
