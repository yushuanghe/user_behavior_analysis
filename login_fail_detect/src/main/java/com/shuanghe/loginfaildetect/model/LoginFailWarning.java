package com.shuanghe.loginfaildetect.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class LoginFailWarning implements Serializable {
    private String uid;
    private long firstFailTime;
    private long lastFailTime;
    private String warningMsg;

    public LoginFailWarning(String uid, long firstFailTime, long lastFailTime, String warningMsg) {
        this.uid = uid;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "LoginFailWarning{" +
                "uid='" + uid + '\'' +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }
}
