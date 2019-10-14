package com.yuyuko.raftkv.remoting.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyServer implements ClientResponseSender {
    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private final ServerBootstrap serverBootstrap;

    private final NettyServerConfig serverConfig;

    private final EventLoopGroup eventLoopGroupBoss;

    private final EventLoopGroup eventLoopGroupSelector;

    private final long id;

    private final ClientRequestHandler handler;

    public NettyServer(long id, final NettyServerConfig serverConfig,
                       ClientRequestProcessor processor) {
        this.serverBootstrap = new ServerBootstrap();
        this.serverConfig = serverConfig;
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private final AtomicInteger cnt = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyNioBoss" + cnt.incrementAndGet());
            }
        });
        this.eventLoopGroupSelector =
                new NioEventLoopGroup(serverConfig.getServerSelectorThreads(), new ThreadFactory() {
                    private final AtomicInteger cnt = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r,
                                "NettyNioSelector-" + serverConfig.getServerSelectorThreads() +
                                        "-" + cnt.incrementAndGet());
                    }
                });
        this.id = id;
        this.handler = new ClientRequestHandler(processor);
    }

    @Override
    public void sendResponseToClient(String requestId, ClientResponse response) {
        ChannelHandlerContext ctx = ClientChannelManager.getInstance().getChannel(requestId);
        if (ctx == null)
            return;
        try {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } catch (Throwable ex) {
            log.warn("[Response to Client Failed],requestId[{}]", requestId,
                    ex);
        } finally {
            ClientChannelManager.getInstance().removeChannel(requestId);
        }
    }

    @ChannelHandler.Sharable
    class ClientRequestHandler extends SimpleChannelInboundHandler<ClientRequest> {
        private final ClientRequestProcessor processor;

        public ClientRequestHandler(ClientRequestProcessor processor) {
            this.processor = processor;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ClientRequest request) throws Exception {
            ClientChannelManager.getInstance().registerChannel(request.getRequestId(), ctx);
            processor.processRequest(request);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.warn("[Exception in ClientRequestHandler]", cause);
            ctx.close();
        }
    }

    public void start() {
        this.serverBootstrap
                .group(eventLoopGroupBoss, eventLoopGroupSelector)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, serverConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, serverConfig.getServerSocketRcvBufSize())
                .localAddress(new InetSocketAddress(this.serverConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(
                                        new HttpResponseEncoder(),
                                        new ClientResponseEncoder(),
                                        new HttpRequestDecoder(),
                                        new HttpObjectAggregator(serverConfig.getMaxContentLength()),
                                        new ClientRequestDecoder(),
                                        handler
                                );
                    }
                });
        ChannelFuture sync;
        try {
            sync = this.serverBootstrap.bind().sync();
            log.info("[Server Bind Success] port {}", serverConfig.getListenPort());
            sync.channel().closeFuture().sync();
        } catch (Throwable ex) {
            log.error("[Server Bind Failed] port {}", serverConfig.getListenPort(), ex);
            System.exit(-1);
        } finally {
            eventLoopGroupBoss.shutdownGracefully();
            eventLoopGroupSelector.shutdownGracefully();
        }
    }
}
