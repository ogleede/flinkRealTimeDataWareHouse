package com.ogleede.gmalllogger.realtime.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Ogleede
 * @Description 商品信息宽表，由于属性多，用构造者模式构建
 *
 * @Builder 注解
 * * 可以使用构造者方式创建对象，给属性赋值
 *
 * @Builder.Default
 * * 在使用构造者方式给属性赋值的时候，属性的初始值会丢失
 * * 该注解的作用就是修复这个问题
 * * 例如：我们在属性上赋值了初始值为 0L，如果不加这个注解，通过构造者创建的
 * * 对象属性值会变为 null
 *
 * @TransientSink 辅助字段，不需要将该字段写入ClickHouse
 *
 * @create 2022-07-02-16:00
 */
@Data
@Builder
public class ProductStats {

    String stt;//窗口起始时间
    String edt; //窗口结束时间

    /**
     * SKU(Stock Keeping Unit)库存量单位，即库存进出计量的单位.
     * 可以是以件、盒、托盘等为单位。
     * SKU是物理上不可分割的最小存货单元。
     * 在使用时要根据不同业态，不同管理模式来处理。
     * iPhone XR 黑色 128gG—— SKU
     */
    Long sku_id; //sku 编号
    String sku_name;//sku 名称
    BigDecimal sku_price; //sku 单价

    /**
     * SPU(Standard Product Unit)：标准化产品单元。
     * 是商品信息聚合的最小单位，是一组可复用、易检索的标准化信息的集合.
     * 描述了一个产品的特性。通俗点讲，属性值、特性相同的商品就可以称为一个SPU。
     * iPhone XR —— SKU
     *
     * SPU是一个商品，而SKU是该商品的不同的规格。
     * 统计销售数据，我们通常用到的是spu，
     * 比如想知道一个产品销售数量多少，通常是看spu就可以很快获取到数据信息；
     * 但如果是需要分析客户和市场，通常会通过sku进行查询，
     * 比如想看一款产品哪个颜色和款式更受买家欢迎，肯定要用的sku。
     */
    Long spu_id; //spu 编号
    String spu_name;//spu 名称

    Long tm_id; //品牌编号
    String tm_name;//品牌名称
    Long category3_id;//品类编号
    String category3_name;//品类名称

    @Builder.Default
    Long display_ct = 0L; //曝光数

    @Builder.Default
    Long click_ct = 0L; //点击数

    @Builder.Default
    Long favor_ct = 0L; //收藏数

    @Builder.Default
    Long cart_ct = 0L; //添加购物车数

    @Builder.Default
    Long order_sku_num = 0L; //下单商品个数

    @Builder.Default //下单商品金额
    BigDecimal order_amount = BigDecimal.ZERO;

    /**
     * 和订单有关的三个指标要做去重
     */
    @Builder.Default
    Long order_ct = 0L; //订单数,要去重

    @Builder.Default //支付金额
    BigDecimal payment_amount = BigDecimal.ZERO;

    @Builder.Default
    Long paid_order_ct = 0L; //支付订单数,要去重

    @Builder.Default
    Long refund_order_ct = 0L; //退款订单数,要去重

    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;

    @Builder.Default
    Long comment_ct = 0L;//评论订单数

    @Builder.Default
    Long good_comment_ct = 0L; //好评订单数

    @Builder.Default
    @TransientSink
    Set orderIdSet = new HashSet(); //用于统计订单数,去重

    @Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数,去重

    @Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();//用于退款支付订单数,去重

    Long ts; //统计时间戳
}
