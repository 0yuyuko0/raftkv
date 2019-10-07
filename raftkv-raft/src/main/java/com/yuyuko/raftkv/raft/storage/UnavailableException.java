package com.yuyuko.raftkv.raft.storage;

public class UnavailableException extends RuntimeException {
    public UnavailableException() {
        super("requested entry at index is unavailable", null, true, false);
    }
}
