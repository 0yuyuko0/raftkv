package com.yuyuko.raftkv.raft.core;

import java.util.Objects;

/**
 * 硬状态需要被保存，包括：节点当前Term、Vote、Commit
 */
public class HardState {
    private long term;

    private long vote;

    private long commit;

    public static final HardState EMPTY = new HardState();

    public HardState() {
    }

    public HardState(long commit, long term, long vote) {
        this.term = term;
        this.vote = vote;
        this.commit = commit;
    }

    public HardState(long term, long vote) {
        this.term = term;
        this.vote = vote;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public long getVote() {
        return vote;
    }

    public void setVote(long vote) {
        this.vote = vote;
    }

    public long getCommit() {
        return commit;
    }

    public void setCommit(long commit) {
        this.commit = commit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HardState hardState = (HardState) o;
        return term == hardState.term &&
                vote == hardState.vote &&
                commit == hardState.commit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, vote, commit);
    }
}
