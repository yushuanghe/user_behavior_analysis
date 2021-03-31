package com.shuanghe.networkanalysis.map;

import com.shuanghe.networkanalysis.model.PvKeyByModel;
import com.shuanghe.networkanalysis.model.RawData6Event;
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
