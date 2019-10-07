package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.remoting.server.ClientRequestDecoder;
import com.yuyuko.raftkv.remoting.server.NettyServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyPeer {
    private static final Logger log = LoggerFactory.getLogger(NettyPeer.class);

    private final NettyPeerConfig clientConfig;

    private final List<Bootstrap> bootstraps = new ArrayList<>();

    private final EventLoopGroup eventLoopGroupWorker;

    private List<PeerNode> peers;

    public NettyPeer(NettyPeerConfig clientConfig, List<PeerNode> peers) {
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
    }

    public void connectToPeers(long localId) {
        if (peers == null) return;
        for (PeerNode peer : peers) {
            if (peer.getId() == localId)
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
                                                new HttpRequestEncoder(),
                                                new PeerMessageEncoder(),
                                                new HttpRequestDecoder(),
                                                new HttpObjectAggregator(clientConfig.getMaxContentLength()),
                                                new ClientRequestDecoder(),
                                                new PeerConnectionHandler(peer.getId()),
                                                new IdleStateHandler(0, 0,
                                                        clientConfig.getClientChannelMaxIdleTimeSeconds()),
                                                NettyServer.getGlobalInstance().getHandler()
                                        );
                            }
                        }).connect().sync();
                log.info("Connect to peer {} success", peer.getId());
            } catch (Throwable e) {
                log.warn("Fail to connect peer {}", peer.getId());
            }
            bootstraps.add(bootstrap);
        }
    }
}
