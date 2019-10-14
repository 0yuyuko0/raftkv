package com.yuyuko.raftkv.server.server;

import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.peer.Cluster;
import com.yuyuko.raftkv.remoting.peer.PeerMessageProcessor;
import com.yuyuko.raftkv.remoting.peer.PeerMessageSender;
import com.yuyuko.raftkv.remoting.peer.PeerNode;
import com.yuyuko.raftkv.remoting.peer.client.NettyPeerClientConfig;
import com.yuyuko.raftkv.remoting.peer.server.NettyPeerServer;
import com.yuyuko.raftkv.remoting.peer.server.NettyPeerServerConfig;
import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.protocol.body.ProposeMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import com.yuyuko.raftkv.remoting.server.*;
import com.yuyuko.raftkv.server.raft.RaftKV;

import java.util.List;

public class Server {
    private final NettyServer server;

    private final Cluster cluster;

    private static volatile Server globalInstance;

    public Server(long id,
                  int port,
                  ClientRequestProcessor requestProcessor,
                  List<PeerNode> peerNodes,
                  PeerMessageProcessor messageProcessor) {
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(port);

        server = new NettyServer(id, serverConfig, requestProcessor);


        NettyPeerServerConfig peerServerConfig = new NettyPeerServerConfig();
        peerServerConfig.setListenPort(port + NettyPeerServerConfig.PEER_PORT_INCREMENT);

        cluster = new Cluster(id, peerServerConfig,
                new NettyPeerClientConfig(), messageProcessor, peerNodes);
        globalInstance = this;
    }

    public void start() {
        cluster.start();
        server.start();
    }

    public static void sendMessageToPeer(List<Message> messages) {
        if (globalInstance != null)
            globalInstance.cluster.sendMessageToPeer(messages);
    }

    public static void sendResponseToClient(String requestId, ClientResponse response) {
        if (globalInstance != null)
            globalInstance.server.sendResponseToClient(requestId, response);
    }

    public static void main(String[] args) {
        ProposeMessage proposeMessage = new ProposeMessage("1", "2");

        byte[] proposal = ProtostuffCodec.getInstance().encode(proposeMessage);
        Message message = new Message();
        message.setEntries(List.of(new Entry(proposal)));

        byte[] bytes = ProtostuffCodec.getInstance().encode(message);
        Message decode = ProtostuffCodec.getInstance().decode(bytes, message.getClass());
    }
}