package com.yuyuko.raftkv.remoting.server;

public class NettyServerConfig {
    private int listenPort = 8888;

    private int serverSelectorThreads = 4;

    private int maxContentLength = 1024 * 1024;

    private int serverSocketSndBufSize = 65535;

    private int serverSocketRcvBufSize = 65535;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }


    public int getServerSelectorThreads() {
        return serverSelectorThreads;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }
}
