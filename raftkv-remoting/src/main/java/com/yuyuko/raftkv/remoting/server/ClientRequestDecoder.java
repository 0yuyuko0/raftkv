package com.yuyuko.raftkv.remoting.server;

import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.peer.PeerMessage;
import com.yuyuko.raftkv.remoting.protocol.body.ProposeMessage;
import com.yuyuko.raftkv.remoting.protocol.body.ReadIndexMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.UUID;

public class ClientRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> {
    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        String uri = msg.uri().substring(1);
        if (msg.method() == HttpMethod.GET) {
            out.add(newRequest(RequestCode.READ_INDEX,
                    new ReadIndexMessage(uri)));
        } else if (msg.method() == HttpMethod.PUT) {
            out.add(newRequest(RequestCode.PROPOSE,
                    new ProposeMessage(uri,
                            msg.content().toString(CharsetUtil.UTF_8))));
        }
    }

    public static ClientRequest newRequest(int code, Object body) {
        return new ClientRequest(UUID.randomUUID().toString(), code,
                ProtostuffCodec.getInstance().encode(body));
    }
}
