package com.yuyuko.raftkv.remoting.peer.server;

import com.yuyuko.raftkv.remoting.peer.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyPeerServer extends AbstractPeerRemoting {
    private static final Logger log = LoggerFactory.getLogger(NettyPeerServer.class);

    private final ServerBootstrap serverBootstrap;

    private final EventLoopGroup eventLoopGroupBoss;

    private final EventLoopGroup eventLoopGroupSelector;

    private final NettyPeerServerConfig config;

    private static volatile NettyPeerServer globalInstance;

    public NettyPeerServer(long id, NettyPeerServerConfig config,
                           PeerMessageProcessor peerMessageProcessor,
                           PeerChannelManager channelManager) {
        super(id, peerMessageProcessor, channelManager);
        this.config = config;
        this.serverBootstrap = new ServerBootstrap();
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private final AtomicInteger cnt = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyPeerServerBoss" + cnt.incrementAndGet());
            }
        });
        this.eventLoopGroupSelector =
                new NioEventLoopGroup(config.getServerSelectorThreads(), new ThreadFactory() {
                    private final AtomicInteger cnt = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r,
                                "NettyPeerSelector-" + config.getServerSelectorThreads() +
                                        "-" + cnt.incrementAndGet());
                    }
                });
        globalInstance = this;
    }

    @Override
    public void start() {
        this.serverBootstrap
                .group(eventLoopGroupBoss, eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, config.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, config.getServerSocketRcvBufSize())
                .localAddress(new InetSocketAddress(config.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new PeerMessageEncoder(),
                                new PeerMessageDecoder(),
                                new PeerServerConnectionHandler(channelManager),
                                new PeerMessageHandler(messageProcessor)
                        );
                    }
                });
        try {
            this.serverBootstrap.bind().sync();
            log.info("[Peer Server Bind Success],port {}", config.getListenPort());
        } catch (Throwable e) {
            log.error("[Peer Server Bind Failed] port {}", config.getListenPort(), e);
            System.exit(-1);
        }
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupBoss.shutdownGracefully();
        this.eventLoopGroupSelector.shutdownGracefully();
    }

    public static NettyPeerServer getGlobalInstance() {
        return globalInstance;
    }
}
