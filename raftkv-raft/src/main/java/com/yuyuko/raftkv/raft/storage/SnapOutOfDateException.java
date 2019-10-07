package com.yuyuko.raftkv.raft.storage;

public class SnapOutOfDateException extends RuntimeException {
    public SnapOutOfDateException() {
    }

    public SnapOutOfDateException(String message) {
        super("requested index is older than the existing snapshot");
    }

    public SnapOutOfDateException(String message, Throwable cause) {
        super(message, cause);
    }

    public SnapOutOfDateException(Throwable cause) {
        super(cause);
    }

    public SnapOutOfDateException(String message, Throwable cause, boolean enableSuppression,
                                  boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
