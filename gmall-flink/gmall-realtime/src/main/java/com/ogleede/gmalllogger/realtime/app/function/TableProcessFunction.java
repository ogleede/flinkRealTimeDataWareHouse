package com.ogleede.gmalllogger.realtime.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ogleede.gmalllogger.realtime.bean.TableProcess;
import com.ogleede.gmalllogger.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


/**
 * @author Ogleede
 * @Description
 * @create 2022-06-03-13:58
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;//可改进池化
    private OutputTag<com.alibaba.fastjson.JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;


    public TableProcessFunction(OutputTag<com.alibaba.fastjson.JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    //value:{"db":"", "tn":"", "before":{}, "after":{},"type":""}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //1.解析数据 String -> TableProcess
        //这张表一般不删，获取新增数据，after一定有值，这里暂不做为空判断
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.检查HBase表是否存在，如果不存在则在Phoenix中建表
        if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {//只有HBase才需要建表
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.写入状态,广播出去（广播由框架底层实现）
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }


    //建表语句：
    //create table if not exists db.tn(id varchar primary key, tm_name varchar) xxx;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if(null == sinkPk) {
                sinkPk = "id";//维表的主键一般都是id
            }

            if(null == sinkExtend) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append(" ( ");
            String[] fields = sinkColumns.split(",");
            for(int i = 0; i < fields.length; ++i) {
                String field = fields[i];

                //判断是否是主键
                if(sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                }else {
                    createTableSQL.append(field).append(" varchar ");
                }
                //判断是否为最后一个字段,如果不是，则添加逗号
                if(i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }
            createTableSQL.append(" ) ").append(sinkExtend);

            System.out.println(createTableSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表 " + sinkTable + " 建表失败！");
            //如果建表失败，再向下走也没有意义，直接抛异常
        } finally {
            if (null != preparedStatement) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //value:{"db":"", "tn":"", "before":{}, "after":{},"type":""}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> collector) throws Exception {
        //1.获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);//可能为null

        if(null != tableProcess) {
            //2.过滤数据-sink columns
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, tableProcess.getSinkColumns());

            //3.分流
            //从广播流的状态中获取sink到哪张表
            value.put("sinkTable", tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //kafka数据，写入主流
                collector.collect(value);
            }else if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //写入侧输出流
                ctx.output(outputTag, value);
            }
        }else {
            System.out.println("该组合key： " + key + " 不存在!");
        }
    }


    /**
     *
     * @param data {"id":"11", "tm_name":"ogleede", "logo_url":"aaa}
     * @param sinkColumns id, tm_name
     *                    {"id":"11", "tm_name":"ogleede"}
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        List<String> columns = Arrays.asList(sinkColumns.split(","));

        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

//        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
//        while(iterator.hasNext()) {
//            Map.Entry<String, Object> next = iterator.next();
//            if(!columns.contains(next.getKey())) {
//                iterator.remove();
//            }
//        }
    }
}
