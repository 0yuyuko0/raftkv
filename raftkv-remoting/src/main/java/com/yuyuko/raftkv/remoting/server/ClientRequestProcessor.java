package com.yuyuko.raftkv.remoting.server;

import io.netty.channel.ChannelHandlerContext;

public interface ClientRequestProcessor {
    void processRequest(ClientRequest clientRequest);
}