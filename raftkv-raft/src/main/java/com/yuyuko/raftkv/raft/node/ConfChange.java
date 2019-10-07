package com.yuyuko.raftkv.raft.node;

import java.io.*;

public class ConfChange implements Serializable {
    public enum ConfChangeType {
        AddNode,
        RemoveNode,
        UpdateNode
    }

    private long id;

    private ConfChangeType type;

    private long nodeId;

    private byte[] context;

    public ConfChange(ConfChangeType type, long nodeId, byte[] context) {
        this(0, type, nodeId, context);
    }

    public ConfChange(long id, ConfChangeType type, long nodeId, byte[] context) {
        this.id = id;
        this.type = type;
        this.nodeId = nodeId;
        this.context = context;
    }

    public long getId() {
        return id;
    }

    public ConfChangeType getType() {
        return type;
    }

    public long getNodeId() {
        return nodeId;
    }

    public byte[] getContext() {
        return context;
    }

    /**
     * 将ConfChange序列化
     *
     * @return
     */
    public byte[] marshal() {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream stream = new ObjectOutputStream(byteStream);
            stream.writeObject(this);
            return byteStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static final ConfChange unmarshal(byte[] bytes) {
        try{
            ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
            return ((ConfChange) new ObjectInputStream(byteStream).readObject());
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
