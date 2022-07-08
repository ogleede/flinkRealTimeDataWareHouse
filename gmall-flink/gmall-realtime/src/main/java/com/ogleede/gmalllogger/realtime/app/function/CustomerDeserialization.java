package com.ogleede.gmalllogger.realtime.app.function;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    //封装的数据格式:json
    //{
    // "database":"",
    // "tableName":"",
    // "type":"c u d",
    // "before":"{"":"","":""......}",
    // "after":"{"":"","":""......}"
    // }
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject res = new JSONObject();

        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();

        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if(before != null) {
            Schema beforeSchema = before.schema();//用schema做循环
            List<Field> beforeFields = beforeSchema.fields();
            for(Field field : beforeFields) {
                beforeJson.put(field.name(), before.get(field));
            }
        }

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if(after != null) {
            Schema afterSchema = after.schema();//用schema做循环
            List<Field> afterFields = afterSchema.fields();
            for(Field field : afterFields) {
                afterJson.put(field.name(), after.get(field));
            }
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//        System.out.println(operation);
        //获取操作类型  CREATE UPDATE DELETE
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        res.put("database", database);
        res.put("tableName", tableName);
        res.put("before", beforeJson);
        res.put("after", afterJson);
        res.put("type", type);


        collector.collect((res.toJSONString()));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
