package com.ogleede.gmalllogger.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Ogleede
 * @Description
 * @create 2022-07-07-23:46
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {

    private String keyword;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}
