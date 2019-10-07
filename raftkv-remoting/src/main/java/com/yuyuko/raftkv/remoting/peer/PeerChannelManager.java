package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.remoting.ChannelManager;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PeerChannelManager implements ChannelManager<Long> {
    private static final Logger log = LoggerFactory.getLogger(PeerChannelManager.class);

    private final Map<Long, ChannelHandlerContext> map = new ConcurrentHashMap<>();

    private static final PeerChannelManager manager = new PeerChannelManager();

    public static PeerChannelManager getInstance() {
        return manager;
    }

    @Override
    public void registerChannel(Long id, ChannelHandlerContext ctx) {
        if (map.putIfAbsent(id, ctx) == null)
            log.info("peer {} connect to server", id);
    }

    @Override
    public ChannelHandlerContext getChannel(Long id) {
        return map.get(id);
    }

    @Override
    public void removeChannel(Long id) {
        map.remove(id);
    }
}
