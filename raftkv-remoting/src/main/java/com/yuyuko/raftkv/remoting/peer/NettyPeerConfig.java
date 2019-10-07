package com.yuyuko.raftkv.remoting.peer;

public class NettyPeerConfig {
    private int connectTimeoutMillis = 3000;

    private long channelNotActiveInterval = 1000 * 60;

    private int clientChannelMaxIdleTimeSeconds = 1;

    private int clientSocketSndBufSize = 65535;

    private int clientSocketRcvBufSize = 65535;

    private int maxContentLength = 1024 * 4;

    public int getClientSocketSndBufSize() {
        return clientSocketSndBufSize;
    }

    public int getClientSocketRcvBufSize() {
        return clientSocketRcvBufSize;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public long getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }

    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }
}
