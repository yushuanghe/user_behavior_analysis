package com.shuanghe.loginfaildetect.model;

import java.io.Serializable;

/**
 * @author yushu
 */
public class LoginEvent implements Serializable {
    private String uid;
    private String ip;
    private String eventType;
    private long timestamp;

    public LoginEvent(String uid, String ip, String eventType, long timestamp) {
        this.uid = uid;
        this.ip = ip;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "uid='" + uid + '\'' +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
