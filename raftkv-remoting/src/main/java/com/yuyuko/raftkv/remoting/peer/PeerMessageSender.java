package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.raft.core.Message;

import java.util.List;

public interface PeerMessageSender {
    void sendMessageToPeer(List<Message> message);
}
