package com.yuyuko.raftkv.raft.core;

@FunctionalInterface
public interface StepFunction {

    void step(Raft raft, Message message);
}
