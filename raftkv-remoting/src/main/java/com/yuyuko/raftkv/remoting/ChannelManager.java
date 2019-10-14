package com.yuyuko.raftkv.remoting;

import io.netty.channel.ChannelHandlerContext;

public interface ChannelManager<T> {
    boolean registerChannel(T id, ChannelHandlerContext ctx);

    ChannelHandlerContext getChannel(T id);

    boolean removeChannel(T id);
}
