package com.yuyuko.raftkv.remoting;

import io.netty.channel.ChannelHandlerContext;

public interface ChannelManager<T> {
    void registerChannel(T id, ChannelHandlerContext ctx);

    ChannelHandlerContext getChannel(T id);

    void removeChannel(T id);
}
