package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class PeerMessageDecoder extends LengthFieldBasedFrameDecoder {

    private static final int FRAME_MAX_LENGTH = 1 << 16;

    public PeerMessageDecoder() {
        super(FRAME_MAX_LENGTH, 0, 2, 0, 2);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = ((ByteBuf) super.decode(ctx, in));
        if (frame == null)
            return null;
        try {
            int len = frame.readableBytes();
            byte[] bytes = new byte[len];
            frame.readBytes(bytes, frame.readerIndex(), len);
            return ProtostuffCodec.getInstance().decode(bytes, PeerMessage.class);
        } finally {
            frame.release();
        }
    }
}
