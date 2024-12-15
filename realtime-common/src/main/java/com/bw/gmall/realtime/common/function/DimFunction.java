package com.bw.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;

/**
 * @className: DimFunction
 * @Description: TODO
 * @author: lx
 * @date: 2024/12/15 11:11
 */
public interface DimFunction <T>{
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}
