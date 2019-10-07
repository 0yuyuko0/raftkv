package com.yuyuko.raftkv.raft.read;

public class ReadState {
    /**
     * 接受该读请求时的commited index
     */
    private long index;

    /**
     * 保存读请求的id，全局唯一
     */
    private byte[] requestCtx;

    public ReadState(long index, byte[] requestCtx) {
        this.index = index;
        this.requestCtx = requestCtx;
    }

    public long getIndex() {
        return index;
    }

    public byte[] getRequestCtx() {
        return requestCtx;
    }
}
