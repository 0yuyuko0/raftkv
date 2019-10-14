package com.yuyuko.raftkv.remoting.peer.client;

import com.yuyuko.raftkv.remoting.peer.PeerChannelManager;
import com.yuyuko.raftkv.remoting.peer.PeerMessage;
import com.yuyuko.raftkv.remoting.peer.PeerReconnectionManager;
import com.yuyuko.raftkv.remoting.peer.server.NettyPeerServer;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerClientConnectionHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(PeerClientConnectionHandler.class);

    private final long peerId;

    private final PeerChannelManager channelManager;

    private final PeerReconnectionManager reconnectionManager;

    public PeerClientConnectionHandler(long peerId, PeerChannelManager channelManager,
                                       PeerReconnectionManager reconnectionManager) {
        this.peerId = peerId;
        this.channelManager = channelManager;
        this.reconnectionManager = reconnectionManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        boolean registered = channelManager.registerChannel(peerId, ctx);

        //已经有channel了，不需要两个连接，关闭掉他
        if (!registered) {
            log.info("[Two Connection], peer {},close one", peerId);
            reconnectionManager.doNotReconnect(peerId);
            ctx.writeAndFlush(PeerMessage.doNotReconnect(NettyPeerServer.getGlobalInstance().getId()));
            return;
        }

        log.info("[Peer connect],peer {}", peerId);

        //连接上后发个心跳
        ctx.writeAndFlush(PeerMessage.heartbeat(NettyPeerServer.getGlobalInstance().getId()));

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (reconnectionManager.reconnect(peerId)) {
            channelManager.removeChannel(peerId);
            log.warn("[Peer disconnect],peer {}", peerId);
        }

        super.channelInactive(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            log.warn("[Peer Idle],peer {}", peerId);
            ctx.writeAndFlush(PeerMessage.heartbeat(NettyPeerServer.getGlobalInstance().getId()))
                    .addListener((ChannelFutureListener) future -> {
                        if (!future.isSuccess()) {
                            future.channel().close();
                            log.warn("[Heartbeat when idle send failed] peer {}, remove channel",
                                    peerId);
                            channelManager.removeChannel(peerId);
                        }
                    });
        } else
            super.userEventTriggered(ctx, evt);
    }

}
