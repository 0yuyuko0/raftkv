package com.yuyuko.raftkv.remoting.server;

public interface ServerMessageSender {
    void sendResponseToClient(String requestId, ClientResponse response);
}
