package com.ogleede.gmalllogger.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.ogleede.gmalllogger.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @author Ogleede
 * @Description 无论是查Phoenix还是MySQL，只要是Jdbc接口，都可以用这个工具类
 * @create 2022-06-10-19:14
 */
public class JdbcUtil {
    /**
     * @param connection        为了适配不同数据库，需要传入connection
     * @param querySql
     * @param clazz             为了方便构建泛型对象，将T的类型传进来
     * @param <T>
     * @param underScoreToCamel 是否将下划线命名方式转化为驼峰命名
     * @return
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clazz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {


        List<T> resList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //解析resultSet
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            //创建泛型对象
            T t = clazz.newInstance();
            for (int i = 1; i < columnCount + 1; i++) {//jdbc中从1开始
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换为驼峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }
                //获取列值
                Object value = resultSet.getObject(i);
                //给泛型对象赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            //添加至结果集
            resList.add(t);
        }

        preparedStatement.close();
        resultSet.close();

        return resList;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        List<JSONObject> queryList = queryList(connection, "select * from GMALL_REALTIME.DIM_USER_INFO", JSONObject.class, true);
        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }
        connection.close();
    }
}
