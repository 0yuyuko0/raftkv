package com.yuyuko.raftkv.remoting.server;

import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.protocol.body.PeerMessage;
import com.yuyuko.raftkv.remoting.protocol.body.ProposeMessage;
import com.yuyuko.raftkv.remoting.protocol.body.ReadIndexMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import java.util.List;
import java.util.UUID;

public class ClientRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> {
    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        String uri = msg.uri().substring(1);
        if (msg.method() == HttpMethod.GET) {
            out.add(newRequest(RequestCode.READ,
                    new ReadIndexMessage(uri)));
        } else if (msg.method() == HttpMethod.PUT) {
            out.add(newRequest(RequestCode.PROPOSE,
                    new ProposeMessage(uri,
                            msg.content().toString(CharsetUtil.UTF_8))));
        } else if (msg.method() == HttpMethod.POST) {
            //from peer
            ByteBuf content = msg.content();
            int len = content.readableBytes();
            byte[] bytes = new byte[len];
            content.getBytes(content.readerIndex(), bytes);
            PeerMessage message = ProtostuffCodec.getInstance().decode(
                    bytes, PeerMessage.class
            );
            out.add(newRequest(message.getCode(), message));
        }
    }

    public static ClientRequest newRequest(int code, Object body) {
        return new ClientRequest(UUID.randomUUID().toString(), code,
                ProtostuffCodec.getInstance().encode(body));
    }
}
