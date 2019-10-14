package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.raft.core.Message;

import java.nio.ByteBuffer;

/**
 * 节点之间通信的消息
 * 为null表示心跳消息（不是raft里的心跳）
 */
public class PeerMessage {
    private Message message;

    private PeerMessageType type;

    public enum PeerMessageType {
        Normal,
        Heartbeat,
        DoNotReconnect
    }

    public PeerMessage() {
    }

    public PeerMessage(Message message) {
        this(message, PeerMessageType.Normal);
    }

    private PeerMessage(Message message, PeerMessageType type) {
        this.message = message;
        this.type = type;
    }

    public static PeerMessage heartbeat(long id) {
        return new PeerMessage(Message.builder().from(id).build(), PeerMessageType.Heartbeat);
    }

    public static PeerMessage doNotReconnect(long id) {
        return new PeerMessage(Message.builder().from(id).build(), PeerMessageType.DoNotReconnect);
    }

    public Message getMessage() {
        return message;
    }

    public PeerMessageType getType() {
        return type;
    }
}
