package com.shuanghe.marketanalysis.model;

import java.io.Serializable;

/**
 * Description:
 * Date: 2021-03-31
 * Time: 20:34
 *
 * @author yushu
 */
public class MarketUserBehavior implements Serializable {
    private String userId;
    private String behavior;
    private String channel;
    private long timestamp;

    public MarketUserBehavior(String userId, String behavior, String channel, long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MarketUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
