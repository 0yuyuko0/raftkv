package com.yuyuko.raftkv.remoting.server;

import io.netty.channel.ChannelHandlerContext;

public interface NettyRequestProcessor {
    NettyRequestProcessor EMPTY = (request,ctx) -> {
    };

    void processRequest(ClientRequest clientRequest, ChannelHandlerContext ctx);
}