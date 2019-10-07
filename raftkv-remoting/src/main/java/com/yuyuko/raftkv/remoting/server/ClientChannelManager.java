package com.yuyuko.raftkv.remoting.server;

import com.yuyuko.raftkv.remoting.ChannelManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientChannelManager implements ChannelManager<String> {
    private final Map<String, ChannelHandlerContext> map = new ConcurrentHashMap<>();

    private static final ClientChannelManager manager = new ClientChannelManager();

    public static ClientChannelManager getInstance() {
        return manager;
    }

    @Override
    public void registerChannel(String requestId, ChannelHandlerContext ctx) {
        map.put(requestId, ctx);
    }

    @Override
    public void removeChannel(String requestId) {
        map.remove(requestId);
    }

    @Override
    public ChannelHandlerContext getChannel(String requestId) {
        return map.get(requestId);
    }
}
