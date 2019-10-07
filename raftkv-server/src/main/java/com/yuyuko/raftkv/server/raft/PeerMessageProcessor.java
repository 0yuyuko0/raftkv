package com.yuyuko.raftkv.server.raft;

import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.remoting.peer.PeerChannelManager;
import com.yuyuko.raftkv.remoting.protocol.body.PeerMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import com.yuyuko.raftkv.remoting.server.ClientRequest;
import com.yuyuko.raftkv.remoting.server.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;

public class PeerMessageProcessor implements NettyRequestProcessor {
    @Override
    public void processRequest(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        PeerMessage peerMessage = ProtostuffCodec.getInstance().decode(clientRequest.getBody(),
                PeerMessage.class);
        Message message = peerMessage.getMessage();
        PeerChannelManager.getInstance().registerChannel(message.getFrom(), ctx);
        RaftNode.getGlobalInstance().getNode().step(message);
    }
}
