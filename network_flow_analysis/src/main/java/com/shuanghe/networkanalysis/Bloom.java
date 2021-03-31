package com.shuanghe.networkanalysis;

import java.io.Serializable;

/**
 * 自定义布隆过滤器
 * 主要是一个bitmap和hash函数
 *
 * @author yushu
 */
public class Bloom implements Serializable {
    /**
     * 默认应该是2的整次幂
     */
    private long size;

    public Bloom(long size) {
        this.size = size;
    }

    /**
     * hash函数
     *
     * @param value
     * @param seed  随机种子
     * @return
     */
    public long hash(String value, int seed) {
        long result = 0;
        for (int i = 0; i < value.length(); i++) {
            result = result * seed + value.charAt(i);
        }
        /*
        返回hash值，要映射到size范围内
        size=00010000
        size-1=00001111
        &操作，&0都是0，&1不变
        相当于把result截取在size范围内的几位
         */
        return (size - 1) & result;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
