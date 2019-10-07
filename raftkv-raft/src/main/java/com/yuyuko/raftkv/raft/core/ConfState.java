package com.yuyuko.raftkv.raft.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConfState {
    private List<Long> nodes = new ArrayList<>();

    public List<Long> getNodes() {
        return nodes;
    }

    public ConfState(List<Long> nodes) {
        this.nodes.addAll(nodes);
    }

    public ConfState() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfState confState = (ConfState) o;
        return Objects.equals(nodes, confState.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes);
    }
}
