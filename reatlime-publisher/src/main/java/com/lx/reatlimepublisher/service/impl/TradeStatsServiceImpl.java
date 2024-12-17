package com.lx.reatlimepublisher.service.impl;


import com.lx.reatlimepublisher.bean.TradeProvinceOrderAmount;
import com.lx.reatlimepublisher.mapper.TradeStatsMapper;
import com.lx.reatlimepublisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/14
 * 交易域统计service接口实现类
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
