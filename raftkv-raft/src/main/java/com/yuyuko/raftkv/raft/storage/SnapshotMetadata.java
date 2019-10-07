package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.core.ConfState;

import java.util.Objects;

public class SnapshotMetadata {
    private ConfState confState = new ConfState();

    private long index;

    private long term;

    public long getIndex() {
        return index;
    }

    public long getTerm(){
        return term;
    }

    public ConfState getConfState() {
        return confState;
    }

    public SnapshotMetadata(ConfState confState, long index, long term) {
        this.confState = confState;
        this.index = index;
        this.term = term;
    }

    public SnapshotMetadata() {
    }

    public SnapshotMetadata(long index) {
        this.index = index;
    }

    public SnapshotMetadata(long index, long term) {
        this.index = index;
        this.term = term;
    }

    public void setConfState(ConfState confState) {
        this.confState = confState;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotMetadata that = (SnapshotMetadata) o;
        return index == that.index &&
                term == that.term &&
                Objects.equals(confState, that.confState);
    }
}
