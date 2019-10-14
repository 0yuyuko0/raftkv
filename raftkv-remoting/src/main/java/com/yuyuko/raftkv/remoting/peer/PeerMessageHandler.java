package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.raft.core.Message;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * 处理peer的消息
 */
@ChannelHandler.Sharable
public class PeerMessageHandler extends SimpleChannelInboundHandler<PeerMessage> {
    private PeerMessageProcessor messageProcessor;

    public PeerMessageHandler(PeerMessageProcessor messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PeerMessage msg) throws Exception {
        if (msg == null)
            return;
        if (msg.getType() == PeerMessage.PeerMessageType.Normal)
            messageProcessor.process(msg.getMessage());
    }
}