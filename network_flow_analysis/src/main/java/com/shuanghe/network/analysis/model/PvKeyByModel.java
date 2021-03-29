package com.shuanghe.network.analysis.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class PvKeyByModel implements Serializable {
    private String key;
    private Long count;

    public PvKeyByModel(String key, Long count) {
        this.key = key;
        this.count = count;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
