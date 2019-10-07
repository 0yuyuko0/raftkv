package com.yuyuko.raftkv.remoting.server;

import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.peer.PeerChannelManager;
import com.yuyuko.raftkv.remoting.peer.PeerConnectionHandler;
import com.yuyuko.raftkv.remoting.peer.PeerMessageEncoder;
import com.yuyuko.raftkv.remoting.peer.PeerMessageSender;
import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.protocol.body.PeerMessage;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyServer implements ServerMessageSender, PeerMessageSender {
    private static final Logger log = LoggerFactory.getLogger(NettyServer.class);

    private final ServerBootstrap serverBootstrap;

    private final NettyServerConfig serverConfig;

    private final EventLoopGroup eventLoopGroupBoss;

    private final EventLoopGroup eventLoopGroupSelector;

    private final Map<Integer, NettyRequestProcessor> processors = new ConcurrentHashMap<>();

    public static volatile NettyServer globalInstance;

    private long id;

    private NettyServerHandler handler;

    public NettyServer(long id, final NettyServerConfig serverConfig) {
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
        this.handler = new NettyServerHandler();
        globalInstance = this;
    }

    @Override
    public void sendMessageToPeer(List<Message> messages) {
        if (messages == null)
            return;
        for (Message message : messages) {
            ChannelHandlerContext ctx =
                    PeerChannelManager.getInstance().getChannel(message.getTo());
            if (ctx == null)
                continue;
            try {
                ctx.writeAndFlush(new PeerMessage(RequestCode.PEER_MESSAGE, message)).addListener(
                        future -> {
                            if (!future.isSuccess()) {
                                ctx.channel().close();
                                PeerChannelManager.getInstance().removeChannel(message.getTo());
                                log.warn("send message to peer{} failed,close channel",
                                        message.getTo());
                            }
                        }
                );
            } catch (Throwable ex) {
                log.warn("send message to peer{} failed", message.getTo(),
                        ex);
            }
        }
    }

    @Override
    public void sendResponseToClient(String requestId, ClientResponse response) {
        ChannelHandlerContext ctx = ClientChannelManager.getInstance().getChannel(requestId);
        if (ctx == null)
            return;
        try {
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        } catch (Throwable ex) {
            log.warn("send response to requestId[{}] failed", requestId,
                    ex);
        } finally {
            ClientChannelManager.getInstance().removeChannel(requestId);
        }
    }

    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<ClientRequest> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ClientRequest msg) throws Exception {
            processMessageReceived(ctx, msg);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.warn("exception in NettyServerHandler", cause);
            ctx.close();
        }
    }

    void processMessageReceived(ChannelHandlerContext ctx, ClientRequest request) {
        switch (request.getCode()) {
            case RequestCode.READ:
            case RequestCode.PROPOSE:
                ClientChannelManager.getInstance().registerChannel(request.getRequestId(), ctx);
                break;
            default:
        }
        NettyRequestProcessor requestProcessor = processors.get(request.getCode());
        requestProcessor.processRequest(request, ctx);
    }

    public void registerProcessor(int code, NettyRequestProcessor processor) {
        this.processors.put(code, processor);
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
                                        new NotAcceptByteBufHttpRequestEncoder(),
                                        new PeerMessageEncoder(),
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
            InetSocketAddress address = (InetSocketAddress) sync.channel().localAddress();
            sync.channel().closeFuture().sync();
        } catch (Throwable ex) {
            log.error("server bind exception", ex);
            System.exit(-1);
        }
    }

    public static NettyServer getGlobalInstance() {
        return globalInstance;
    }

    public long getId() {
        return id;
    }

    public NettyServerHandler getHandler() {
        return handler;
    }
}
