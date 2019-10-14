package com.yuyuko.raftkv.remoting.peer.server;

public class NettyPeerServerConfig {
    public static final int PEER_PORT_INCREMENT = 10000;

    private int listenPort = 18888;

    private int serverSelectorThreads = 4;

    private int serverSocketSndBufSize = 65535;

    private int serverSocketRcvBufSize = 65535;

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getListenPort() {
        return listenPort;
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
}
