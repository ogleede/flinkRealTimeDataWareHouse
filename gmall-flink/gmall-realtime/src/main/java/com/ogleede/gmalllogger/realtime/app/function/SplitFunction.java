package com.ogleede.gmalllogger.realtime.app.function;

import com.ogleede.gmalllogger.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author Ogleede
 * @Description 自定义UDTF，相当于DataStream中的flatmap
 *  UDF:    用户定义（普通）函数，只对单行数值产生作用
 *  UDTF:   用户定义表生成函数，用来解决输入一行输出多行
 *  UDAF:   用户定义聚合函数，可对多行数据产生作用
 * @create 2022-07-07-22:06
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String str) {
        //分词
        List<String> words = KeywordUtil.splitKeyword(str);
        //遍历写出
        for(String word : words) {
            collect(Row.of(word));
        }
    }
}
