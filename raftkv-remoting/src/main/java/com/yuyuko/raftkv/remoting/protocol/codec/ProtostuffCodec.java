package com.yuyuko.raftkv.remoting.protocol.codec;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProtostuffCodec implements JavaCodec {
    private static final ThreadLocal<LinkedBuffer> buffer = new ThreadLocal<>();

    private static final ProtostuffCodec codec = new ProtostuffCodec();

    public static ProtostuffCodec getInstance() {
        return codec;
    }

    private static LinkedBuffer getLinkedBuffer() {
        LinkedBuffer linkedBuffer = buffer.get();
        if (linkedBuffer != null)
            return linkedBuffer;
        LinkedBuffer newBuffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        buffer.set(newBuffer);
        return newBuffer;
    }

    @Override
    public <T> byte[] encode(T o) {
        Objects.requireNonNull(o);
        byte[] bytes = null;
        @SuppressWarnings("unchecked")
        Schema<T> schema = (Schema<T>) RuntimeSchema.getSchema(o.getClass());
        LinkedBuffer buffer = getLinkedBuffer();
        bytes = ProtostuffIOUtil.toByteArray(o, schema, buffer);
        buffer.clear();
        return bytes;
    }

    @Override
    public <T> T decode(byte[] bytes, Class<T> clazz) {
        Objects.requireNonNull(clazz);
        if (bytes == null)
            return null;
        T o;
        Schema<T> schema = RuntimeSchema.getSchema(clazz);
        o = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, o, schema);
        return o;
    }

    public static class SimpleObject {
        int a;
        float b;

        public SimpleObject(int a, float b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public String toString() {
            return "SimpleObject{" +
                    "a=" + a +
                    ", b=" + b +
                    '}';
        }
    }

    public static void main(String[] args) {

        byte[] bytes = ProtostuffCodec.getInstance().encode(List.of(new SimpleObject(1, 2),
                new SimpleObject(2, 3)));
        ProtostuffCodec.getInstance().decode(bytes, List.class).forEach(System.out::println);
    }
}
