package com.shuanghe.marketanalysis.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class BlackListUserWarning implements Serializable {
    private String uid;
    private String appId;
    private String msg;

    public BlackListUserWarning(String uid, String appId, String msg) {
        this.uid = uid;
        this.appId = appId;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BlackListUserWarning{" +
                "uid='" + uid + '\'' +
                ", appId='" + appId + '\'' +
                ", msg='" + msg + '\'' +
                '}';
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
