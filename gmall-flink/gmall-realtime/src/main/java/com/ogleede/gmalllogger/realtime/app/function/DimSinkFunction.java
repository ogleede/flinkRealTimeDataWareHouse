package com.ogleede.gmalllogger.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.common.GmallConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author Ogleede
 * @Description 这里继承了RichSinkFunction，可以利用其open方法，在初始化时连接Phoenix
 * @create 2022-06-04-13:15
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //Phoenix不像Mysql自动提交，可以在这里设置自己提交，也可以在invoke函数后面提交。
        connection.setAutoCommit(true);
    }

    /**
     * @param value
     * value:{"sinkTable":"dim_base_trademark",
     *        "database":"gmall-flink",
     *        "before":{},
     *        "after":{"tm_name":"insert-test","id":12},
     *        "type":"insert",
     *        "tableName":"base_trademark"}
     * @Description : invoke函数是sink函数操作每一个数据的函数
     * @SQL: upsert into db.tn(id,tm_name) values('...','...')
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句
            String upsertSql = genUpsertSql(
                    value.getString("sinkTable"),
                    value.getJSONObject("after"));
            System.out.println(upsertSql);
            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行插入操作
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(null != preparedStatement) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSql(String sinkTable, JSONObject after) {
        Set<String> keySet = after.keySet();
        Collection<Object> values = after.values();

        //利用StringUtils.join()来实现和scala中类似的mkString()操作
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + StringUtils.join(keySet, ",") + ") values ('"
                + StringUtils.join(values, "','") + "')";
    }
}
