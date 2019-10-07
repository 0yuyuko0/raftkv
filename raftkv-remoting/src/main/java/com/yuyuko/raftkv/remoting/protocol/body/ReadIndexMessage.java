package com.yuyuko.raftkv.remoting.protocol.body;

public class ReadIndexMessage {
    private String key;

    public ReadIndexMessage(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
