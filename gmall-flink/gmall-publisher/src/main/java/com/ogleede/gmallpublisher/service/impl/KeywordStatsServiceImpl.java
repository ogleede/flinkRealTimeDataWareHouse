package com.ogleede.gmallpublisher.service.impl;

import com.ogleede.gmallpublisher.bean.KeywordStats;
import com.ogleede.gmallpublisher.mapper.KeywordStatsMapper;
import com.ogleede.gmallpublisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Desc:关键词统计接口实现类
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
