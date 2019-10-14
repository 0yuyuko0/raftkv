package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.peer.client.NettyPeerClient;
import com.yuyuko.raftkv.remoting.peer.client.NettyPeerClientConfig;
import com.yuyuko.raftkv.remoting.peer.server.NettyPeerServer;
import com.yuyuko.raftkv.remoting.peer.server.NettyPeerServerConfig;

import java.util.List;
import java.util.stream.Collectors;

public class Cluster implements PeerMessageSender {
    private NettyPeerServer server;

    private NettyPeerClient client;

    public Cluster(long id,
                   NettyPeerServerConfig serverConfig,
                   NettyPeerClientConfig clientConfig,
                   PeerMessageProcessor processor,
                   List<PeerNode> peerNodes) {
        PeerChannelManager channelManager = new PeerChannelManager();
        server = new NettyPeerServer(id, serverConfig, processor, channelManager);
        client = new NettyPeerClient(id, clientConfig, peerNodes, processor, channelManager);
    }

    public void start() {
        server.start();
        client.start();
    }

    public void shutdown() {
        server.shutdown();
        client.shutdown();
    }

    @Override
    public void sendMessageToPeer(List<Message> messages) {
        if (messages != null || messages.size() > 0) {
            client.sendMessage(messages);
        }
    }
}