package com.yuyuko.raftkv.raft.storage;

import java.util.Arrays;

public class Snapshot {
    private byte[] data;

    private SnapshotMetadata metadata = new SnapshotMetadata();

    public Snapshot(byte[] data, SnapshotMetadata metadata) {
        this.data = data;
        this.metadata = metadata;
    }

    public Snapshot() {
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public SnapshotMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(SnapshotMetadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Snapshot snapshot = (Snapshot) o;
        return Arrays.equals(data, snapshot.data) &&
                metadata.equals(snapshot.metadata);
    }
}
