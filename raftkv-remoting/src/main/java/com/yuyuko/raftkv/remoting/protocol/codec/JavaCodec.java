package com.yuyuko.raftkv.remoting.protocol.codec;

public interface JavaCodec {
    <T> byte[] encode(T o);

    <T> T decode(byte[] bytes, Class<T> clazz);
}
