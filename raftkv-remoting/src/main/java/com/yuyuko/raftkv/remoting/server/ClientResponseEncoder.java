package com.yuyuko.raftkv.remoting.server;

import com.yuyuko.raftkv.remoting.protocol.ResponseCode;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

@ChannelHandler.Sharable
public class ClientResponseEncoder extends MessageToMessageEncoder<ClientResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, ClientResponse msg, List<Object> out)
            throws Exception {
        out.add(newHttpResponse(msg.getBody()));
        ReferenceCountUtil.release(msg);
    }

    private static FullHttpResponse newHttpResponse(byte[] body) {
        DefaultFullHttpResponse response =
                new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                        Unpooled.copiedBuffer(body));
        response.headers().add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        return response;
    }
}
