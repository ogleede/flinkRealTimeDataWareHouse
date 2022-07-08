package com.ogleede.sql;

/**
 * @author Ogleede
 * @Description
 * @create 2022-07-04-0:54
 */
public class testSQL {
    public static void main(String[] args) {
        String sql = "select " +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "province_id, " +
                "province_name, " +
                "province_area_code, " +
                "province_iso_code, " +
                "province_3166_2_code, " +
                "COUNT(DISTINCT order_id) order_count, " +
                "sum(split_total_amount) order_amount, " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from " +
                "order_wide " +
                "group by " +
                "TUMBLE(rt, INTERVAL '10' SECOND), " +
                "province_id, " +
                "province_name, " +
                "province_area_code, " +
                "province_iso_code, " +
                "province_3166_2_code)";
        System.out.println("the length() of sql = " + sql.length());
    }
}
