package com.shuanghe.network.analysis.map;

import com.shuanghe.network.analysis.model.PvKeyByModel;
import com.shuanghe.network.analysis.model.RawData6Event;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 生成随机key
 *
 * @author yushu
 */
public class MyMapper implements MapFunction<RawData6Event, PvKeyByModel> {
    @Override
    public PvKeyByModel map(RawData6Event value) throws Exception {
        return new PvKeyByModel(RandomStringUtils.randomAlphanumeric(32), 1L);
    }
}
