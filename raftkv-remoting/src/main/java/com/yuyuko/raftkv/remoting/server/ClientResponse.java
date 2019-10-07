package com.yuyuko.raftkv.remoting.server;

public class ClientResponse {
    private int code;

    private byte[] body;

    public ClientResponse(int code, byte[] body) {
        this.code = code;
        this.body = body;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }


}
