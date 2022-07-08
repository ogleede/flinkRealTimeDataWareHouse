package com.ogleede.gmalllogger.realtime.bean;

import lombok.Data;
import java.math.BigDecimal;

/**
 * @author Ogleede
 * @Description 订单实体类，字段来自于MySQL
 * @create 2022-06-07-20:37
 */
@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;//yyyy-MM-dd HH:mm:ss
    String operate_time;

    String create_date; // 以下三个是扩展字段，由create_time得到
    String create_hour;
    Long create_ts;
}
