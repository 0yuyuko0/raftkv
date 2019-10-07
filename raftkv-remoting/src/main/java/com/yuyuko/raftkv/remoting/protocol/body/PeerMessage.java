package com.yuyuko.raftkv.remoting.protocol.body;

import com.yuyuko.raftkv.raft.core.Message;

public class PeerMessage {
    private int code;

    private Message message;

    public PeerMessage(int code, Message message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public Message getMessage() {
        return message;
    }
}
