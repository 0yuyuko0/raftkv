package com.yuyuko.raftkv.raft.core;

import com.yuyuko.raftkv.raft.node.Node;

import java.util.Objects;

/**
 * 软状态是异变的，包括：当前集群leader、当前节点状态
 * 这部分数据不需要存储到持久化中
 */
public class SoftState {
    long lead;

    Node.NodeState nodeState;

    public SoftState(long lead, Node.NodeState nodeState) {
        this.lead = lead;
        this.nodeState = nodeState;
    }

    public long getLead() {
        return lead;
    }

    public Node.NodeState getNodeState() {
        return nodeState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SoftState softState = (SoftState) o;
        return lead == softState.lead &&
                nodeState == softState.nodeState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lead, nodeState);
    }
}
