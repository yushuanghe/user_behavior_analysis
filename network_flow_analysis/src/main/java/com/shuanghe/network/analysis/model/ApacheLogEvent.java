package com.shuanghe.network.analysis.model;

/**
 * 输入数据
 *
 * @author yushu
 */
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private long timestamp;
    private String method;
    private String url;

    public ApacheLogEvent(String ip, String userId, long timestamp, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.timestamp = timestamp;
        this.method = method;
        this.url = url;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
