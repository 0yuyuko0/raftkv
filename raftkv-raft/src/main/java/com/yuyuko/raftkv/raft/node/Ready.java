package com.yuyuko.raftkv.raft.node;

import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.raft.storage.Snapshot;
import com.yuyuko.raftkv.raft.utils.Utils;

import java.util.List;
import java.util.Objects;

/**
 * 应用层在写入一段数据之后，Raft库将返回这样一个Ready结构体，其中可能某些字段是空的，
 * 毕竟不是每次改动都会导致Ready结构体中的成员都发生变化，此时使用者就需要根据情况，
 * 取出其中不为空的成员进行操作。
 */
public class Ready {
    /**
     * 当前节点的易变的状态，
     * 如果未更新则为null
     */
    private SoftState softState;

    /**
     * 需要写入到持久化存储中的数据
     */
    private HardState hardState;

    /**
     * 需保存ready状态的readindex数据信息
     */
    private List<ReadState> readStates;

    /**
     * 需要在消息发送之前被写入到持久化存储中的entries数据数组
     */
    private List<Entry> entries;

    /**
     * 需要写入到持久化存储中的快照数据
     * 快照数据仅当当前节点在接收从leader发送过来的快照数据时存在，
     * 在接收快照数据的时候，entries数组中是没有数据的；
     * 除了这种情况之外，就只会存在entries数组的数据了。
     */
    private Snapshot snapshot;

    /**
     * 需要输入到状态机中的数据，这些数据之前已经被保存到持久化存储中了
     */
    private List<Entry> committedEntries;

    /**
     * 在entries被写入持久化存储中以后，需要发送出去的数据
     */
    private List<Message> messages;

    public Ready(Raft raft, SoftState prevSoftState, HardState prevHardState) {
        entries = raft.getRaftLog().unstableEntries();
        // 保存committed但是还没有applied的数据数组
        committedEntries = raft.getRaftLog().nextEntries();
        messages = raft.getMessages();
        SoftState softState = raft.softState();
        if (!softState.equals(prevSoftState))
            this.softState = softState;
        HardState hardState = raft.hardState();
        if (!hardState.equals(prevHardState))
            this.hardState = hardState;
        if (!Utils.isEmptySnapshot(raft.getRaftLog().getUnstable().getSnapshot()))
            snapshot = raft.getRaftLog().getUnstable().getSnapshot();
        if (Utils.notEmpty(raft.getReadStates()))
            readStates = raft.getReadStates();
    }

    Ready() {
    }

    public boolean containsUpdate() {
        return softState != null || !Utils.isEmptyHardState(hardState) || !Utils.isEmptySnapshot(snapshot) || Utils.notEmpty(entries)
                || Utils.notEmpty(committedEntries) || Utils.notEmpty(messages) || Utils.notEmpty(readStates);
    }

    public SoftState getSoftState() {
        return softState;
    }

    public HardState getHardState() {
        return hardState;
    }

    public List<ReadState> getReadStates() {
        return readStates;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public List<Entry> getCommittedEntries() {
        return committedEntries;
    }

    public List<Message> getMessages() {
        return messages;
    }

    public Ready setSoftState(SoftState softState) {
        this.softState = softState;
        return this;
    }

    public Ready setHardState(HardState hardState) {
        this.hardState = hardState;
        return this;
    }

    public Ready setReadStates(List<ReadState> readStates) {
        this.readStates = readStates;
        return this;
    }

    public Ready setEntries(List<Entry> entries) {
        this.entries = entries;
        return this;
    }

    public Ready setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        return this;
    }

    public Ready setCommittedEntries(List<Entry> committedEntries) {
        this.committedEntries = committedEntries;
        return this;
    }

    public Ready setMessages(List<Message> messages) {
        this.messages = messages;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Ready ready = (Ready) o;
        return Objects.equals(softState, ready.softState) &&
                Objects.equals(hardState, ready.hardState) &&
                Utils.isCollectionEquals(readStates, ready.readStates) &&
                Utils.isCollectionEquals(entries, ready.entries) &&
                Objects.equals(snapshot, ready.snapshot) &&
                Utils.isCollectionEquals(committedEntries, ready.committedEntries) &&
                Utils.isCollectionEquals(messages, ready.messages);
    }


    @Override
    public int hashCode() {
        return Objects.hash(softState, hardState, readStates, entries, snapshot, committedEntries
                , messages);
    }
}