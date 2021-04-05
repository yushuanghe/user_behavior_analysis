package com.shuanghe.orderoaydetect.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class OrderResult implements Serializable {
    private String orderId;
    private String resultMsg;

    public OrderResult(String orderId, String resultMsg) {
        this.orderId = orderId;
        this.resultMsg = resultMsg;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId='" + orderId + '\'' +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }
}
