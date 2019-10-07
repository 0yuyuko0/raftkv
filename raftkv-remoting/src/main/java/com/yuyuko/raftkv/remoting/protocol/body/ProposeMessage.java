package com.yuyuko.raftkv.remoting.protocol.body;

public class ProposeMessage {
    private String key;

    private String value;

    public ProposeMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
