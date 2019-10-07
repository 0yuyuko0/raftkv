package com.yuyuko.raftkv.raft.node;

public class Peer {
    private Long id;

    private byte[]context;

    public Peer(Long id, byte[] context) {
        this.id = id;
        this.context = context;
    }

    public Long getId() {
        return id;
    }

    public byte[] getContext() {
        return context;
    }
}
