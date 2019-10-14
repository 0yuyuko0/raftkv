package com.yuyuko.raftkv.remoting.peer.client;

public class NettyPeerClientConfig {
    private int connectTimeoutMillis = 3000;

    private int maxReconnectTimes = 3;

    private int reconnectThreadPoolSize = 4;

    private int reconnectPeriodSeconds = 1;

    private int clientChannelMaxIdleTimeSeconds = 10;

    private int clientSocketSndBufSize = 65535;

    private int clientSocketRcvBufSize = 65535;

    public int getMaxReconnectTimes() {
        return maxReconnectTimes;
    }

    public int getReconnectThreadPoolSize() {
        return reconnectThreadPoolSize;
    }

    public int getReconnectPeriodSeconds() {
        return reconnectPeriodSeconds;
    }

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public int  getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }
}
