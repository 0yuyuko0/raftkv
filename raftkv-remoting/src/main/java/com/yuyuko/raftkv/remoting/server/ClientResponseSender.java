package com.yuyuko.raftkv.remoting.server;

public interface ClientResponseSender {
    void sendResponseToClient(String requestId, ClientResponse response);
}
