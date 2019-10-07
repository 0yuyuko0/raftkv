package com.yuyuko.raftkv.raft.node;

import com.yuyuko.raftkv.raft.core.HardState;
import com.yuyuko.raftkv.raft.core.Progress;
import com.yuyuko.raftkv.raft.core.SoftState;

import java.util.Map;
import java.util.Objects;

public class Status {
    private long id;

    private HardState hardState;

    private SoftState softState;

    private long applied;

    private Map<Long, Progress> progresses;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public HardState getHardState() {
        return hardState;
    }

    public void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    public SoftState getSoftState() {
        return softState;
    }

    public void setSoftState(SoftState softState) {
        this.softState = softState;
    }

    public long getApplied() {
        return applied;
    }

    public void setApplied(long applied) {
        this.applied = applied;
    }

    public Map<Long, Progress> getProgresses() {
        return progresses;
    }

    public void setProgresses(Map<Long, Progress> progresses) {
        this.progresses = progresses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Status status = (Status) o;
        return id == status.id &&
                applied == status.applied &&
                Objects.equals(hardState, status.hardState) &&
                Objects.equals(softState, status.softState) &&
                Objects.equals(progresses, status.progresses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, hardState, softState, applied, progresses);
    }
}
