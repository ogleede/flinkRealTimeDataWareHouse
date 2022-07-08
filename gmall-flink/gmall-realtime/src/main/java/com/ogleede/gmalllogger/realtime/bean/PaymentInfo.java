package com.ogleede.gmalllogger.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author Ogleede
 * @Description
 * @create 2022-06-29-0:15
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
