package com.ogleede.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Description;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController//@Controller + @ResponseBody
@Slf4j
public class LoggerController {//返回值是一个页面

    @Autowired//自动注入
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test1")//后面访问页面时，直接访问这个test1，那么就访问到了当前方法。
//    @ResponseBody//不返回页面，返回的是java对象
    public String test1() {
        System.out.println("succ");
        return "succ";
    }

    @RequestMapping("test2")
    @Description("在url上用？后面接name=xxx来得到信息，多个参数之间用&连接")
    public String test2(@RequestParam("name") String nn,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(nn + ":" + age);
        return "succ";
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {
        //打印数据
//        System.out.println(jsonStr);

        //将数据落盘
//        log.debug(jsonStr);
        log.info(jsonStr);
//        log.warn(jsonStr);
//        log.error(jsonStr);
//        log.trace(jsonStr);


        //将数据写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
