package com.ogleede.gmalllogger.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.common.GmallConfig;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Ogleede
 * @Description 慢，会造成反压，需要优化
 * //缓存 ：一级缓存用Redis，二级缓存用内存缓存（LRUCache）
 * @create 2022-06-10-20:44
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        //拼接SQL
        //select * from db.tn where id = '18';
        String querySQL = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName +
                " where id = '" + id + "'";

        //查询Phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySQL, JSONObject.class, false);
        return queryList.get(0);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
//        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1378"));//441
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1221"));//365

        long end1 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "1221"));//365

        long end2 = System.currentTimeMillis();
        connection.close();
        System.out.println(end1 - start);
        System.out.println(end2 - start);
    }
}
