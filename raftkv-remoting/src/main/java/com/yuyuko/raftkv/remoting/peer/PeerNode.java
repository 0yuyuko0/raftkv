package com.yuyuko.raftkv.remoting.peer;

public class PeerNode {
    private long id;

    private String addr;

    private int port;

    public PeerNode(long id, String addr, int port) {
        this.id = id;
        this.addr = addr;
        this.port = port;
    }

    public long getId() {
        return id;
    }

    public String getAddr() {
        return addr;
    }

    public int getPort() {
        return port;
    }
}
