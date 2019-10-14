package com.yuyuko.raftkv.remoting.peer.server;

import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.peer.PeerChannelManager;
import com.yuyuko.raftkv.remoting.peer.PeerMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;

@ChannelHandler.Sharable
public class PeerServerConnectionHandler extends SimpleChannelInboundHandler<PeerMessage> {
    private final PeerChannelManager channelManager;

    public PeerServerConnectionHandler(PeerChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PeerMessage msg) throws Exception {
        if (msg == null)
            return;
        long from = msg.getMessage().getFrom();

        switch (msg.getType()) {
            case Normal:
            case Heartbeat:
                channelManager.registerChannel(from, ctx);
                ctx.fireChannelRead(msg);
                break;
            case DoNotReconnect:
                ctx.channel().close();
                break;
        }
    }
}