package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

public class PeerMessageEncoder extends MessageToByteEncoder<PeerMessage> {
    @Override
    protected void encode(ChannelHandlerContext ctx, PeerMessage msg, ByteBuf out) throws Exception {
        byte[] bytes = ProtostuffCodec.getInstance().encode(msg);
        short length = 2;
        ByteBuffer res = ByteBuffer.allocate(length + bytes.length);
        res.putShort(((short) bytes.length));
        res.put(bytes);
        res.flip();
        out.writeBytes(res);
    }
}
