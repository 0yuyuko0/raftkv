package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.core.Raft;
import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * raft日志的相关操作
 */
public class RaftLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftLog.class);

    private Storage storage;

    /**
     * 保存还没有持久化的数据
     */
    private Unstable unstable;

    /**
     * 保存当前提交的日志数据索引
     */
    private long committed;

    /**
     * 保存已经应用到状态机的数据的最高索引
     */
    private long applied;

    public static RaftLog newRaftLog(Storage storage) {
        if (storage == null) {
            throw new RuntimeException("storage must not be null");
        }
        RaftLog raftLog = new RaftLog();
        raftLog.setStorage(storage);
        long firstIndex = storage.firstIndex();
        long lastIndex = storage.lastIndex();

        Unstable unstable = new Unstable();
        // offset从持久化之后的最后一个index的下一个开始
        unstable.setOffset(lastIndex + 1);
        raftLog.unstable = unstable;
        // committed和applied从持久化的第一个index的前一个开始
        raftLog.committed = firstIndex - 1;
        raftLog.applied = firstIndex - 1;
        return raftLog;
    }

    public static RaftLog newRaftLog(Storage storage, Unstable unstable, long committed) {
        if (storage == null) {
            throw new RuntimeException("storage must not be null");
        }
        RaftLog raftLog = new RaftLog();
        raftLog.setStorage(storage);
        raftLog.setUnstable(unstable);
        raftLog.setCommitted(committed);
        return raftLog;
    }

    /**
     * @return raftLog中的所有entry
     */
    public List<Entry> allEntries() {
        try {
            return entries((int) firstIndex(), Raft.UNLIMIT);
        } catch (CompactedException e) {/// try again if there was a racing compaction
            return allEntries();
        }
    }

    /**
     * @param entries 要添加的日志条目
     * @return 最后一条日志的索引
     */
    public long append(List<Entry> entries) {
        if (Utils.isEmpty(entries))
            return lastIndex();
        // 如果索引小于committed，则说明该数据是非法的
        long firstEntryIndex = entries.get(0).getIndex();
        if (firstEntryIndex - 1 < committed) {
            LOGGER.error("entry({}) is out of range [committed({})]", firstEntryIndex, committed);
            throw new RaftException();
        }
        // 放入unstable存储中
        unstable.truncateAndAppend(entries);
        return lastIndex();
    }

    /**
     * 返回快照数据
     *
     * @return snapshot
     */
    public Snapshot snapshot() {
        if (unstable.getSnapshot() != null) {
            // 如果没有保存的数据有快照，就返回
            return unstable.getSnapshot();
        }
        return storage.snapshot();
    }

    /**
     * 获取从i开始的entries返回，大小不超过maxsize
     *
     * @param index   开始的index
     * @param maxSize 最大大小
     * @return
     */
    public List<Entry> entries(int index, long maxSize) {
        if (index > lastIndex())
            return List.of();
        return slice(index, lastIndex() + 1, maxSize);
    }

    /**
     * 传入数据索引，该索引表示在这个索引之前的数据应用层都进行了持久化，修改unstable的数据
     *
     * @param index index
     * @param term  term
     */
    public void stableTo(long index, long term) {
        unstable.stableTo(index, term);
    }

    /**
     * 返回[lo,hi-1]之间的数据，这些数据的大小总和不超过maxSize
     *
     * @param lo      上界
     * @param hi      下界 -1
     * @param maxSize 最大总数据大小
     * @return [lo, hi-1]之间的Entry
     */
    public List<Entry> slice(long lo, long hi, long maxSize) {
        checkOutOfBounds(lo, hi);
        if (lo == hi)
            return new ArrayList<>();
        List<Entry> res = null;
        // lo 小于unstable的offset，说明前半部分在持久化的storage中
        if (lo < unstable.getOffset()) {
            // 注意传入storage.Entries的hi参数取hi和unstable offset的较小值
            List<Entry> storeEntries;
            try {
                storeEntries = storage.entries(lo, Math.min(hi, unstable.getOffset()),
                        maxSize);
            } catch (CompactedException e) {
                throw e;
            } catch (UnavailableException e) {
                LOGGER.error("entries[{}:{}) is unavailable from storage", lo, Math.min(hi,
                        unstable.getOffset()));
                throw new RaftException();
            } catch (Throwable e) {
                throw new RaftException(e);
            }
            if (storeEntries.size() < Math.min(hi, unstable.getOffset()) - lo)
                return storeEntries;
            res = storeEntries;
        }
        if (hi > unstable.getOffset()) {
            // hi大于unstable offset，说明后半部分在unstable中取得
            List<Entry> unstableEntries = unstable.slice(Math.max(lo, unstable.getOffset()), hi);
            if (res != null) {
                res.addAll(unstableEntries);
            } else
                res = unstableEntries;
        }
        return Utils.limitSize(res, maxSize);
    }

    public void checkOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            LOGGER.error("invalid slice {} > {}", lo, hi);
            throw new RaftException();
        }
        long fi = firstIndex();
        if (lo < fi)
            throw new CompactedException();
        long len = lastIndex() + 1 - fi;
        if (lo < fi || hi > fi + len) {
            LOGGER.error("slice[{},{}) out of bound [{},{}]", lo, hi, fi, lastIndex());
            throw new RaftException();
        }
    }

    /**
     * @return 最后一条日志的索引
     */
    public long lastIndex() {
        long lastIndex = unstable.maybeLastIndex();
        if (lastIndex != 0)
            return lastIndex;
        return storage.lastIndex();
    }

    /**
     * @return 第一条日志的索引
     */
    public long firstIndex() {
        // 首先尝试在未持久化数据中看有没有快照数据
        long unstableFirstIndex = unstable.maybeFirstIndex();
        if (unstableFirstIndex != 0)
            return unstableFirstIndex;
        // 否则才返回持久化数据的firstIndex
        return storage.firstIndex();
    }

    public long term(long index) {
        //合法的范围是[index of dummy entry, last index]
        long dummy = firstIndex() - 1;

        // 先判断范围是否正确
        if (index < dummy || index > lastIndex())
            return 0;

        long unstableTerm = unstable.maybeTerm(index);
        // 尝试从unstable中查询term
        if (unstableTerm != 0)
            return unstableTerm;

        return storage.term(index);
    }

    /**
     * @return 最后一条日志的term
     */
    public long lastTerm() {
        try {
            return term(lastIndex());
        } catch (Throwable ex) {
            LOGGER.error("unexpected error when getting the last term");
            throw new RaftException(ex);
        }
    }

    public void appliedTo(long applied) {
        if (applied == 0)
            return;
        if (committed < applied || applied < this.applied) {
            LOGGER.error("applied{} is out of range [prevApplied{}, committed{}]", applied,
                    this.applied, committed);
            throw new RaftException();
        }
        this.applied = applied;
    }

    public boolean maybeCommit(long index, long term) {
        // 只有在传入的index大于当前commit索引，以及index对应的term与传入的term匹配时，才使用这些数据进行commit
        if (index > committed && zeroTermOnErrCompacted(index) == term) {
            commitTo(index);
            return true;
        }
        return false;
    }

    public void commitTo(long toCommit) {
        // 首先需要判断，commit索引绝不能变小
        if (committed < toCommit) {
            // 传入的值如果比lastIndex大则是非法的
            if (lastIndex() < toCommit) {
                LOGGER.error("tocommit({}) is out of range [lastIndex({})]. " +
                        "Was the raft log corrupted, truncated, or lost?", toCommit, lastIndex());
                throw new RaftException();
            }
            committed = toCommit;
            LOGGER.info("commit to {}", toCommit);
        }
    }

    /**
     * 尝试添加一组日志，如果不能添加则返回(0,false)，否则返回(新的日志的索引,true)
     *
     * @param index     从哪里开始的日志条目
     * @param logTerm   这一组日志对应的term
     * @param committed leader上的committed索引
     * @param entries   需要提交的一组日志，因此这组数据的最大索引为index+len(ents)
     * @return
     */
    public long maybeAppend(long index, long logTerm, long committed, List<Entry> entries) {
        if (term(index) == logTerm) {
            if (entries == null)
                entries = List.of();
            // 首先需要保证传入的index和logTerm能匹配的上才能走入这里，否则直接返回false

            // 首先得到传入数据的最后一条索引
            long lastEntryIndex = index + entries.size();
            // 查找传入的数据从哪里开始找不到对应的Term了
            long conflictIndex = findConflict(entries);
            if (conflictIndex == 0)
                ;
            else if (conflictIndex <= this.committed) {
                // 找到的数据索引小于committed，都说明传入的数据是错误的
                LOGGER.error("entry {} conflict with committed entry [committed({})]",
                        conflictIndex, this.committed);
                throw new RaftException();
            } else {
                // ci > 0的情况下来到这里
                long offset = index + 1;
                // 从查找到的数据索引开始，将这之后的数据放入到unstable存储中
                append(entries.subList(((int) (conflictIndex - offset)), entries.size()));
            }
            // 选择committed和lastnewi中的最小者进行commit
            commitTo(Math.min(committed, lastEntryIndex));
            return lastEntryIndex;
        }
        return 0;
    }

    /**
     * 返回第一个在entry数组中，index中的term与当前存储的数据不同的索引
     * 如果没有冲突数据，而且当前存在的日志条目包含所有传入的日志条目，返回0；
     * 如果没有冲突数据，而且传入的日志条目有新的数据，则返回新日志条目的第一条索引
     * 一个日志条目在其索引值对应的term与当前相同索引的term不相同时认为是有冲突的数据。
     *
     * @param entries
     * @return index中的term与当前存储的数据不同的索引
     */
    public long findConflict(List<Entry> entries) {
        for (Entry entry : entries) {
            // 找到第一个任期号不匹配的，即当前在raftLog存储的该索引数据的任期号，不是ent数据的任期号
            if (term(entry.getIndex()) != entry.getTerm()) {
                if (entry.getIndex() <= lastIndex()) {
                    // 如果不匹配任期号的索引数据，小于当前最后一条日志索引，就打印错误日志
                    LOGGER.info("found conflict at index {} [existing term: {}, conflicting term:" +
                                    " {}]", entry.getIndex(),
                            zeroTermOnErrCompacted(entry.getIndex()),
                            entry.getTerm());
                }
                return entry.getIndex();
            }
        }
        return 0;
    }


    /**
     * 如果term函数抛出CompactedException异常，则返回0
     *
     * @param index
     * @return index对应的term
     */
    public long zeroTermOnErrCompacted(long index) {
        try {
            return term(index);
        } catch (CompactedException ex) {
            return 0;
        }
    }


    // 判断是否比当前节点的日志更新：1）term是否更大 2）term相同的情况下，索引是否更大
    public boolean isUpToDate(long index, long term) {
        return term > lastTerm() || (term == lastTerm() && index >= lastIndex());
    }


    public void restore(Snapshot s) {
        LOGGER.info(" starts to restore snapshot [index: {}, term: {}]",
                s.getMetadata().getIndex(), s.getMetadata().getTerm());
        committed = s.getMetadata().getIndex();
        unstable.restore(s);
    }

    /**
     *
     * @return 返回unstable存储的数据
     */
    public List<Entry> unstableEntries() {
        if (Utils.isEmpty(unstable.getEntries()))
            return null;
        return unstable.getEntries();
    }


    public Storage getStorage() {
        return storage;
    }

    public Unstable getUnstable() {
        return unstable;
    }

    public long getCommitted() {
        return committed;
    }

    public long getApplied() {
        return applied;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public void setUnstable(Unstable unstable) {
        this.unstable = unstable;
    }

    public void setCommitted(long committed) {
        this.committed = committed;
    }

    public void setApplied(long applied) {
        this.applied = applied;
    }

    /**
     *
     * @return 返回commit但是还没有apply的所有数据
     */
    public List<Entry> nextEntries() {
        long offset = Math.max(applied + 1, firstIndex());
        if (committed + 1 > offset) {
            // 如果commit索引比前面得到的值还大，说明还有没有commit了但是还没apply的数据，将这些数据返回
            try {
                return slice(offset, committed + 1, Raft.UNLIMIT);
            } catch (Throwable ex) {
                LOGGER.error("unexpected error when getting unapplied entries ()");
                throw ex;
            }
        }
        return null;
    }

    public void stableSnapTo(long snapIdx) {
        unstable.stableSnapTo(snapIdx);
    }
}
