package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.core.HardState;
import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.utils.Tuple;
import com.yuyuko.raftkv.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 内存中的Storage的实现
 */

public class MemoryStorage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryStorage.class);

    private HardState hardState;

    private Snapshot snapshot = new Snapshot();

    private List<Entry> entries = new ArrayList<>();

    public MemoryStorage() {
    }

    public MemoryStorage(HardState hardState, List<Entry> entries) {
        this.hardState = hardState;
        this.entries = entries;
    }

    public static MemoryStorage newMemoryStorage() {
        MemoryStorage storage = new MemoryStorage();
        storage.entries.add(new Entry());
        return storage;
    }

    public MemoryStorage(List<Entry> entries) {
        this.entries.addAll(entries);
    }

    public synchronized void append(List<Entry> entries) {
        if (Utils.isEmpty(entries))
            return;
        long firstIndex = firstIndex0();
        long lastIndex = entries.get(0).getIndex() + entries.size() - 1;
        if (lastIndex < firstIndex)
            return;
        // 如果当前已经包含传入数据中的一部分，那么已经有的那部分数据可以不用重复添加进来
        if (firstIndex > entries.get(0).getIndex()) {
            int overlap = (int) (firstIndex - entries.get(0).getIndex());
            if (overlap < entries.size())
                entries = entries.subList(overlap,
                        entries.size());
            else
                entries = List.of();
        }

        //计算传入数据到当前已经保留数据的偏移量
        long offset = entries.get(0).getIndex() - this.entries.get(0).getIndex();
        if (this.entries.size() > offset) {
            // 如果当前数据量大于偏移量，说明offset之前的数据从原有数据中取得，之后的数据从传入的数据中取得
            this.entries = new ArrayList<>(this.entries.subList(0, ((int) offset)));
            this.entries.addAll(entries);
        } else if (this.entries.size() == offset) {
            // offset刚好等于当前数据量，说明传入的数据刚好紧挨着当前的数据，所以直接添加进来就好了
            this.entries.addAll(entries);
        } else {
            LOGGER.error("missing log entry [last: {}, append at: {}]", lastIndex0(),
                    entries.get(0).getIndex());
            throw new RaftException();
        }
    }

    /**
     * 根据传入的数据创建快照并且返回
     *
     * @return 创建的快照
     */
    public synchronized Snapshot createSnapshot(long index, ConfState cs, byte[] data) {
        if (index <= snapshot.getMetadata().getIndex())
            throw new SnapOutOfDateException();
        long offset = entries.get(0).getIndex();
        if (index > lastIndex0()) {
            LOGGER.error("snapshot {} is out of bound lastindex({})", index, lastIndex0());
            throw new RaftException();
        }
        // 更新快照中的数据
        snapshot.getMetadata().setIndex(index);
        snapshot.getMetadata().setTerm(entries.get(((int) (index - offset))).getTerm());
        if (cs != null)
            snapshot.getMetadata().setConfState(cs);
        snapshot.setData(data);
        return snapshot;
    }

    /**
     * 根据快照数据进行还原
     */
    public synchronized void applySnapshot(Snapshot snapshot) {
        long index = this.snapshot.getMetadata().getIndex();
        if (index >= snapshot.getMetadata().getIndex())
            throw new SnapOutOfDateException();
        this.snapshot = snapshot;
        this.entries = new ArrayList<>();
        // 这里也插入了一条空数据
        this.entries.add(new Entry(snapshot.getMetadata().getIndex(), snapshot.getMetadata().getTerm()
        ));

    }

    @Override
    public Tuple<HardState, ConfState> initialState() {
        return new Tuple<>(hardState, snapshot.getMetadata().getConfState());
    }

    @Override
    public synchronized List<Entry> entries(long lo, long hi, long maxSize) {
        long offset = entries.get(0).getIndex();
        if (lo <= offset)
            throw new CompactedException();
        if (hi > lastIndex() + 1) {
            LOGGER.error("entries' hi{} is out of bound lastindex{}", hi, lastIndex());
            throw new RaftException();
        }
        if (entries.size() == 1)
            throw new UnavailableException();
        return Utils.limitSize(entries.subList(((int) (lo - offset)), ((int) (hi - offset))),
                maxSize);
    }

    @Override
    public synchronized long term(long index) {
        long offset = entries.get(0).getIndex();
        // 如果比当前数据最小的索引还小，说明已经被compact过了
        if (index < offset)
            throw new CompactedException();
        // 如果超过当前数组大小，返回不可用
        if (index - offset >= entries.size())
            throw new UnavailableException();
        return entries.get(((int) (index - offset))).getTerm();
    }

    @Override
    public synchronized long firstIndex() {
        return firstIndex0();
    }

    private long firstIndex0() {
        return entries.get(0).getIndex() + 1;
    }

    @Override
    public synchronized long lastIndex() {
        return lastIndex0();
    }

    private long lastIndex0() {
        return entries.get(0).getIndex() + entries.size() - 1;
    }

    @Override
    public synchronized Snapshot snapshot() {
        return snapshot;
    }

    /**
     * 数据压缩，将compactIndex之前的数据丢弃掉
     *
     * @param compactIndex 小于compactIndex
     */
    public synchronized void compact(long compactIndex) {
        long firstIdx = entries.get(0).getIndex();
        if (compactIndex <= firstIdx)
            throw new CompactedException();
        if (compactIndex > lastIndex0()) {
            LOGGER.error("compact {} is out of bound lastindex({})", compactIndex, lastIndex0());
            throw new RaftException();
        }
        long idx = compactIndex - firstIdx;
        List<Entry> newEntries = new ArrayList<>(((int) (1 + entries.size() - idx)));
        Entry firstEntry = entries.get(((int) idx));
        newEntries.add(new Entry(firstEntry.getIndex(), firstEntry.getTerm()));
        newEntries.addAll(entries.subList(((int) idx) + 1, entries.size()));
        entries = newEntries;
    }

    public HardState getHardState() {
        return hardState;
    }

    public void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public List<Entry> getEntries() {
        return entries;
    }
}
