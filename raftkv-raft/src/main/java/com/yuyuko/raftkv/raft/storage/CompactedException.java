package com.yuyuko.raftkv.raft.storage;

public class CompactedException extends RuntimeException {
    public CompactedException() {
        super("requested index is unavailable due to compaction", null, true, false);
    }
}
