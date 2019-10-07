package com.yuyuko.raftkv.raft.read;

import com.yuyuko.raftkv.raft.core.Message;

import java.util.HashSet;
import java.util.Set;

public class ReadIndexStatus {
    // 保存原始的readIndex请求消息
    private Message request;

    // 保存收到该readIndex请求时的leader commit索引
    private long index;

    // 保存有什么节点进行了应答，从这里可以计算出来是否有超过半数应答了
    private Set<Long> acks = new HashSet<>();

    public ReadIndexStatus(Message request, long index) {
        this.request = request;
        this.index = index;
    }

    public Message getRequest() {
        return request;
    }

    public long getIndex() {
        return index;
    }

    public Set<Long> getAcks() {
        return acks;
    }
}
