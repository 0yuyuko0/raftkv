package com.yuyuko.raftkv.remoting.server;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequestEncoder;

public class NotAcceptByteBufHttpRequestEncoder extends HttpRequestEncoder {
    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        boolean accept = super.acceptOutboundMessage(msg);
        if (msg instanceof ByteBuf && ((ByteBuf) msg).readableBytes() > 0)
            return false;
        return accept;
    }
}
