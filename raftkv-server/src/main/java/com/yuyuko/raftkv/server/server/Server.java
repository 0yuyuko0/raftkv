package com.yuyuko.raftkv.server.server;

import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.server.NettyRequestProcessor;
import com.yuyuko.raftkv.remoting.server.NettyServer;
import com.yuyuko.raftkv.remoting.server.NettyServerConfig;
import com.yuyuko.raftkv.server.raft.PeerHeartbeatProcessor;
import com.yuyuko.raftkv.server.raft.PeerMessageProcessor;
import com.yuyuko.raftkv.server.raft.RaftKV;

public class Server {
    private NettyServerConfig nettyServerConfig;

    private NettyServer server;

    public Server(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
    }

    public void init(long id) {
        this.server = new NettyServer(id, nettyServerConfig);

        server.registerProcessor(RequestCode.READ, RaftKV.getInstance());
        server.registerProcessor(RequestCode.PROPOSE, RaftKV.getInstance());

        server.registerProcessor(RequestCode.PEER_MESSAGE, new PeerMessageProcessor());
        server.registerProcessor(RequestCode.PEER_HEARTBEAT, new PeerHeartbeatProcessor());
    }

    public void start() {
        if (server != null)
            server.start();
    }
}