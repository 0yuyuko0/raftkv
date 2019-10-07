package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.remoting.protocol.body.PeerMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

public class PeerMessageEncoder extends MessageToMessageEncoder<PeerMessage> {
    @Override
    protected void encode(ChannelHandlerContext ctx, PeerMessage msg, List<Object> out) throws Exception {
        ByteBuf content = Unpooled.copiedBuffer(ProtostuffCodec.getInstance().encode(msg));
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.POST, "/peer",
                content);
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH,
                content.readableBytes());
        out.add(request);
        ReferenceCountUtil.release(msg);
    }
}
