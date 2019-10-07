package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.protocol.body.PeerMessage;
import com.yuyuko.raftkv.remoting.server.NettyServer;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerConnectionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(PeerConnectionHandler.class);

    private long peerId;

    public PeerConnectionHandler(long peerId) {
        this.peerId = peerId;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        PeerChannelManager.getInstance().registerChannel(peerId, ctx);
        log.info("peer {} channel active", peerId);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        PeerChannelManager.getInstance().removeChannel(peerId);
        log.warn("peer {} channel inactive", peerId);
        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            log.warn("peer {} is idle", peerId);
            ctx.writeAndFlush(new PeerMessage(RequestCode.PEER_HEARTBEAT, null))
                    .addListener((ChannelFutureListener) future -> {
                        if (!future.isSuccess()) {
                            future.channel().close();
                            PeerChannelManager.getInstance().removeChannel(peerId);
                        }
                    });
        } else
            super.userEventTriggered(ctx, evt);
    }

}
