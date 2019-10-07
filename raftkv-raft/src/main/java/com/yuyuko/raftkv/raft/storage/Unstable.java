package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * unstable数据结构用于还没有被用户层持久化的数据，而其中又包括两部分：快照数据和日志条目数组
 * 这两个部分，并不同时存在，同一时间只有一个部分存在。
 * 快照数据仅当当前节点在接收从leader发送过来的快照数据时存在，在接收快照数据的时候，
 * entries数组中是没有数据的；除了这种情况之外，就只会存在entries数组的数据了。
 */
public class Unstable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Unstable.class);

    /**
     * 保存还没有持久化的快照数据
     * 在启动之后同步数据的过程中，如果需要同步快照数据时才会去进行赋值修改的数据
     */
    private Snapshot snapshot = new Snapshot();

    /**
     * 还未持久化的数据
     */
    private List<Entry> entries = new ArrayList<>();

    /**
     * 保存的是entries数组中的第一条数据在raft日志中的索引，
     * 即第i条entries数据在raft日志中的索引为i + offset。
     */
    private long offset;

    public Unstable(Snapshot snapshot, List<Entry> entries, long offset) {
        this.snapshot = snapshot;
        this.entries = new ArrayList<>(entries);
        this.offset = offset;
    }

    public Unstable() {
    }

    public Unstable(long offset) {
        this.offset = offset;
    }

    /**
     * 传入entries，可能会导致原先数据的截断或者添加操作
     * 这就是最开始注释中说明的offset可能比持久化索引小的情况，需要做截断
     *
     * @param ents 需要添加的entries
     */
    public void truncateAndAppend(List<Entry> ents) {
        long firstEntryIndex = ents.get(0).getIndex();
        if (firstEntryIndex == offset + this.entries.size()) {
            // 如果正好是紧接着当前数据的，就直接append
            this.entries.addAll(ents);
        } else if (firstEntryIndex <= offset) {
            LOGGER.info("replace the unstable entries from index {}", firstEntryIndex);
            // 如果比当前偏移量小，那用新的数据替换当前数据，需要同时更改offset和entries
            offset = firstEntryIndex;
            this.entries = new ArrayList<>(ents);
        } else {
            // 到了这里，说明 u.offset < firstEntryIndex < u.offset+uint64(len(u.entries))
            // 那么新的entries需要拼接而成
            LOGGER.info("truncate the unstable entries before index {}", firstEntryIndex);
            this.entries = new ArrayList<>(slice(offset, firstEntryIndex));
            this.entries.addAll(ents);
        }
    }

    /**
     * 传入索引i和term，表示目前这块数据已经持久化了
     *
     * @param index 索引
     * @param term  任期
     */
    public void stableTo(long index, long term) {
        long gTerm = maybeTerm(index);
        //只有在term相同，同时索引大于等于当前offset的情况下
        if (gTerm == term && index >= offset) {
            // 因为前面的数据被持久化了，所以将entries缩容，从i开始
            entries = new ArrayList<>(entries.subList(((int) (index + 1 - offset)),
                    entries.size()));
            LOGGER.info("stable to {}, entries size: {}", index, entries.size());
            offset = index + 1;
        }
    }

    /**
     * @param lo 左区间，包括
     * @param hi 右区间，不包括
     * @return 返回索引范围在[lo-u.offset : hi-u.offset)之间的数据
     */
    public List<Entry> slice(long lo, long hi) {
        checkOutOfBounds(lo, hi);
        return new ArrayList<>(entries.subList(((int) (lo - offset)), ((int) (hi - offset))));
    }

    private void checkOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            LOGGER.error("invalid unstable.slice {} > {}", lo, hi);
            throw new RaftException();
        }

        long upper = offset + entries.size();
        if (lo < offset || hi > upper) {
            LOGGER.error("unstable.slice[{},{}) out of bound [{},{}]", lo, hi, offset, upper);
            throw new RaftException();
        }
    }

    /**
     * 只有在快照存在的情况下，返回快照中的meta数据
     * 注意到与取lastIndex值不同的是，firstindex只会从快照中尝试。
     * 这是因为快照中的数据用于刚启动时，此时可能是最早的index，
     * 而entry数组存在数据时，持久化中已经有数据了，此时不可能从entry数组中取firstindex
     *
     * @return
     */
    public long maybeFirstIndex() {
        if (snapshot != null) {
            long index = snapshot.getMetadata().getIndex();
            if (index != 0)
                return index + 1;
        }
        return 0;
    }

    /**
     * 如果entries存在，返回最后一个entry的索引
     * 否则如果快照存在，返回快照的meta数据中的索引
     * 以上都不成立，则返回0
     *
     * @return
     */
    public long maybeLastIndex() {
        if (Utils.notEmpty(entries))
            return offset + entries.size() - 1;
        if (snapshot != null) {
            long index = snapshot.getMetadata().getIndex();
            if (index != 0)
                return index;
        }
        return 0;
    }

    /**
     * 返回索引i的term，如果不存在则返回0
     *
     * @param index 索引
     * @return 索引对应的日志的term
     */
    public long maybeTerm(long index) {
        if (index < offset) {
            // 如果索引比offset小，那么尝试从快照中获取
            if (snapshot == null)
                return 0;
            // 只有在正好快照meta数据的index的情况下才查得到，在这之前的都查不到term了
            if (snapshot.getMetadata().getIndex() == index)
                return snapshot.getMetadata().getTerm();
            return 0;
        }
        long lastIndex = maybeLastIndex();
        if (lastIndex == 0)
            return 0;
        //如果比lastIndex还大，那也查不到
        if (index > lastIndex)
            return 0;
        return entries.get(((int) (index - offset))).getTerm();
    }

    public void stableSnapTo(long snapIdx) {
        if(snapshot != null && snapshot.getMetadata().getIndex() == snapIdx){
            snapshot = null;
        }
    }

    public void restore(Snapshot s) {
        offset = s.getMetadata().getIndex() + 1;
        entries = new ArrayList<>();
        snapshot = s;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public long getOffset() {
        return offset;
    }
}
