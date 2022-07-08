package com.ogleede.gmalllogger.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Ogleede
 * @Description
 * @create 2022-07-07-0:04
 */
public class KeywordUtil {

    /**
     * 如果分词失败，其实可以直接把原词返回
     * 因为多一个词少一个词对整体影响不大，不应该因为分词失败让任务挂掉
     * @param text
     * @return
     */
    //使用 IK 分词器对字符串进行分词
    public static List<String> splitKeyword(String text) {
        /**
         * 把输入字符串转换为字符串输入流，传给IK
         */
        StringReader sr = new StringReader(text);

        /**
         * 两种分词模式
         * smart：每个字用一次，尽量把词变得特别大
         * maxWord：尽量拆的散，只要是词就拆开
         */
        IKSegmenter ik = new IKSegmenter(sr, false);
        Lexeme lex = null;

        //创建集合用户存放结果数据
        List<String> keywordList = new ArrayList();
        while (true) {
            try {
                if ((lex = ik.next()) != null) {
                    String word = lex.getLexemeText();
                    keywordList.add(word);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return keywordList;
    }
    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信 4G 手机 双卡双待";
        System.out.println(KeywordUtil.splitKeyword(text));
    }
}
