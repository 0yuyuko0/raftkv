package com.yuyuko.raftkv.remoting.peer.client;

import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.peer.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyPeerClient extends AbstractPeerRemoting {
    private static final Logger log = LoggerFactory.getLogger(NettyPeerClient.class);

    private final NettyPeerClientConfig clientConfig;

    private final Map<Long, Bootstrap> bootstraps = new HashMap<>();

    private final EventLoopGroup eventLoopGroupWorker;

    private final List<PeerNode> peers;

    private volatile static NettyPeerClient globalInstance;

    public NettyPeerClient(long id, NettyPeerClientConfig clientConfig, List<PeerNode> peers,
                           PeerMessageProcessor peerMessageProcessor,
                           PeerChannelManager channelManager) {
        super(id, peerMessageProcessor, channelManager);
        this.clientConfig = clientConfig;
        this.peers = peers;
        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,
                        String.format("NettyPeerSelector",
                                this.threadIndex.incrementAndGet()));
            }
        });
        globalInstance = this;
    }

    @Override
    public void start() {
        if (peers == null) return;

        PeerReconnectionManager reconnectionManager = new PeerReconnectionManager(clientConfig);

        for (PeerNode peer : peers) {
            if (peer.getId() == id)
                continue;
            Bootstrap bootstrap = new Bootstrap();
            try {
                bootstrap.group(this.eventLoopGroupWorker)
                        .channel(NioSocketChannel.class)
                        .remoteAddress(new InetSocketAddress(peer.getAddr(), peer.getPort()))
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, false)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                                clientConfig.getConnectTimeoutMillis())
                        .option(ChannelOption.SO_SNDBUF, clientConfig.getClientSocketSndBufSize())
                        .option(ChannelOption.SO_RCVBUF, clientConfig.getClientSocketRcvBufSize())
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline()
                                        .addLast(
                                                new PeerMessageEncoder(),
                                                new PeerMessageDecoder(),
                                                new IdleStateHandler(0, 0,
                                                        clientConfig.getClientChannelMaxIdleTimeSeconds()),
                                                new PeerClientConnectionHandler(peer.getId(),
                                                        channelManager, reconnectionManager),
                                                new PeerMessageHandler(messageProcessor)
                                        );
                            }
                        }).connect().sync();
                log.info("[Init Connect Success], connect to peer {},addr {}",
                        peer.getId(), peer.getAddr() + ":" + peer.getPort());
            } catch (Throwable e) {
                log.warn("[Init Connect Failed], fail to connect peer {},addr{}", peer.getId(),
                        peer.getAddr() + ":" + peer.getPort());
            }
            bootstraps.put(peer.getId(), bootstrap);
        }
    }

    public void reconnect(long peerId) throws InterruptedException {
        Bootstrap bootstrap = bootstraps.get(peerId);
        bootstrap.connect().sync();
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupWorker.shutdownGracefully();
    }

    public static NettyPeerClient getGlobalInstance() {
        return globalInstance;
    }

    public void sendMessage(List<Message> messages) {
        for (Message message : messages) {
            ChannelHandlerContext ctx = channelManager.getChannel(message.getTo());
            if (ctx == null)
                continue;
            ChannelFuture channelFuture = ctx.writeAndFlush(new PeerMessage(message));
            channelFuture.addListener(
                    future -> {
                        if (!future.isSuccess()) {
                            channelManager.removeChannel(message.getTo());
                            ctx.channel().close();
                        }
                    }
            );
        }
    }
}
