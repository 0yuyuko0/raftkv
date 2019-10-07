package com.yuyuko.raftkv.remoting.server;

public class ClientRequest {
    private String requestId;

    private int code;

    private byte[] body;

    public ClientRequest(String requestId, int code, byte[] body) {
        this.requestId = requestId;
        this.code = code;
        this.body = body;
    }

    public String getRequestId() {
        return requestId;
    }

    public int getCode() {
        return code;
    }

    public byte[] getBody() {
        return body;
    }
}