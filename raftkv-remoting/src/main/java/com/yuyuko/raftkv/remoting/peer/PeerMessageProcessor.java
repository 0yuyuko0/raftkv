package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.raft.core.Message;

public interface PeerMessageProcessor {
    void process(Message message);
}
