package com.yuyuko.raftkv.raft.storage;

public class SnapshotTemporarilyUnavailableException extends RuntimeException {
    public SnapshotTemporarilyUnavailableException() {
        super("snapshot is temporarily unavailable");
    }
}
