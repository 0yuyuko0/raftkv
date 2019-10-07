package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;

import static com.yuyuko.raftkv.raft.core.Raft.UNLIMIT;
import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {

    @Test
    void findConflict() {
        class Test {
            List<Entry> entries;
            long wConflict;

            public Test(List<Entry> entries, long wConflict) {
                this.entries = entries;
                this.wConflict = wConflict;
            }
        }
        List<Entry> prevEntries = List.of(new Entry(1, 1), new Entry(2, 2), new Entry(3, 3));
        Test[] tests = new Test[]{
                // no conflict, empty ent
                new Test(List.of(), 0),
                // no conflict
                new Test(List.of(new Entry(1, 1), new Entry(2, 2), new Entry(3, 3)), 0),
                new Test(List.of(new Entry(2, 2), new Entry(3, 3)), 0),
                new Test(List.of(new Entry(3, 3)), 0),
                // no conflict, but has new wEnts
                new Test(List.of(new Entry(1, 1), new Entry(2, 2), new Entry(3, 3),
                        new Entry(4, 4), new Entry(5, 4)), 4),
                new Test(List.of(new Entry(2, 2), new Entry(3, 3),
                        new Entry(4, 4), new Entry(5, 4)), 4),
                new Test(List.of(new Entry(3, 3), new Entry(4, 4),
                        new Entry(5, 4)), 4),
                new Test(List.of(new Entry(4, 4), new Entry(5, 4)), 4),
                // conflicts with existing wEnts
                new Test(List.of(new Entry(1, 4), new Entry(2, 4)), 1),
                new Test(List.of(new Entry(2, 1), new Entry(3, 4),
                        new Entry(4, 4)), 2),
                new Test(List.of(new Entry(3, 1), new Entry(4, 2),
                        new Entry(5, 4), new Entry(6, 4)), 3),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            RaftLog raftLog = RaftLog.newRaftLog(MemoryStorage.newMemoryStorage());
            raftLog.append(prevEntries);
            assertEquals(test.wConflict, raftLog.findConflict(test.entries), "" + i);
        }
    }

    @Test
    void isUpToDate() {
        class Test {
            long lastIdx, term;
            boolean wUpToDate;

            public Test(long lastIdx, long term, boolean wUpToDate) {
                this.lastIdx = lastIdx;
                this.term = term;
                this.wUpToDate = wUpToDate;
            }
        }
        List<Entry> prevEntries = List.of(new Entry(1, 1), new Entry(2, 2), new Entry(3, 3));
        RaftLog raftLog = RaftLog.newRaftLog(MemoryStorage.newMemoryStorage());
        raftLog.append(prevEntries);
        Test[] tests = new Test[]{
                // greater term, ignore lastIndex
                new Test(raftLog.lastIndex() - 1, 4, true),
                new Test(raftLog.lastIndex(), 4, true),
                new Test(raftLog.lastIndex() + 1, 4, true),
                // smaller term, ignore lastIndex
                new Test(raftLog.lastIndex() - 1, 2, false),
                new Test(raftLog.lastIndex(), 2, false),
                new Test(raftLog.lastIndex() + 1, 2, false),
                new Test(raftLog.lastIndex() - 1, 3, false),
                new Test(raftLog.lastIndex(), 3, true),
                new Test(raftLog.lastIndex() + 1, 3, true),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            assertEquals(test.wUpToDate, raftLog.isUpToDate(test.lastIdx, test.term));
        }
    }

    @Test
    void maybeAppend() {
        class Test {
            long logTerm, index, commited;
            List<Entry> wEnts;
            long wLastI, wCommit;
            boolean wPanic;

            public Test(long logTerm, long index, long commited, List<Entry> wEnts, long wLastI
                    , long wCommit, boolean wPanic) {
                this.logTerm = logTerm;
                this.index = index;
                this.commited = commited;
                this.wEnts = wEnts;
                this.wLastI = wLastI;
                this.wCommit = wCommit;
                this.wPanic = wPanic;
            }
        }
        List<Entry> prevEntries = List.of(new Entry(1, 1), new Entry(2, 2),
                new Entry(3, 3));
        long lastIdx = 3;
        long lastTerm = 3;
        long commit = 1;
        Test[] tests = new Test[]{
                // not match: term is different
                new Test(lastTerm - 1, lastIdx, lastIdx,
                        List.of(new Entry(lastIdx + 1, 4)), 0, commit, false),
                // not match: index out of bound
                new Test(lastTerm - 1, lastIdx + 1, lastIdx,
                        List.of(new Entry(lastIdx + 2, 4)), 0, commit, false),
                // match with the last existing entry
                new Test(lastTerm, lastIdx, lastIdx,
                        null, lastIdx, lastIdx, false),
                // do not increase commit higher than lastnewi
                new Test(lastTerm, lastIdx, lastIdx + 1,
                        null, lastIdx, lastIdx, false),
                // commit up to the commit in the message
                new Test(lastTerm, lastIdx, lastIdx - 1,
                        null, lastIdx, lastIdx - 1, false),
                // commit do not decrease
                new Test(lastTerm, lastIdx, 0,
                        null, lastIdx, commit, false),
                // commit do not decrease
                new Test(0, 0, lastIdx,
                        null, 0, commit, false),

        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            RaftLog raftLog = RaftLog.newRaftLog(MemoryStorage.newMemoryStorage());
            raftLog.append(prevEntries);
            raftLog.setCommitted(commit);
            Executable testExec = () -> {
                assertEquals(test.wLastI, raftLog.maybeAppend(test.index, test.logTerm,
                        test.commited,
                        test.wEnts));
            };
            if (test.wPanic)
                assertThrows(Throwable.class, testExec, "" + i);
            else
                assertDoesNotThrow(testExec, "" + i);
            assertEquals(test.wCommit, raftLog.getCommitted(), "" + i);
            if (test.wLastI != 0 && Utils.notEmpty(test.wEnts)) {
                assertArrayEquals(test.wEnts.toArray(),
                        raftLog.slice(raftLog.lastIndex() - test.wEnts.size() + 1,
                                raftLog.lastIndex() + 1
                                , UNLIMIT).toArray(), "" + i);
            }
        }
    }

    @Test
    void append() {
        class Test {
            List<Entry> entries, wEntries;
            long wIdx, wUnstable;

            public Test(List<Entry> entries, List<Entry> wEntries, long wIdx, long wUnstable) {
                this.entries = entries;
                this.wEntries = wEntries;
                this.wIdx = wIdx;
                this.wUnstable = wUnstable;
            }
        }
        Test[] tests = new Test[]{
                new Test(List.of(),
                        List.of(new Entry(1, 1), new Entry(2, 2)),
                        2,
                        3),
                new Test(List.of(new Entry(3, 2)),
                        List.of(new Entry(1, 1), new Entry(2, 2),
                                new Entry(3, 2)),
                        3,
                        3),
                // conflicts with index 1
                new Test(List.of(new Entry(1, 2)),
                        List.of(new Entry(1, 2)),
                        1,
                        1),
                // conflicts with index 2
                new Test(List.of(new Entry(2, 3), new Entry(3, 3)),
                        List.of(new Entry(1, 1), new Entry(2, 3),
                                new Entry(3, 3)),
                        3,
                        2)
        };
        List<Entry> prevEntries = List.of(new Entry(1, 1), new Entry(2, 2));
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            MemoryStorage storage = MemoryStorage.newMemoryStorage();
            storage.append(prevEntries);
            RaftLog raftLog = RaftLog.newRaftLog(storage);

            int finalI = i;
            assertAll(
                    () -> assertEquals(test.wIdx, raftLog.append(test.entries), String.format(
                            "#%d" +
                                    ": " +
                                    "index want %d", finalI, test.wIdx)),
                    () -> assertArrayEquals(test.wEntries.toArray(), raftLog.entries(1,
                            UNLIMIT).toArray(), String.format("#%d wEnts", finalI)),
                    () -> assertEquals(test.wUnstable, raftLog.getUnstable().getOffset(),
                            String.format("#%d unstable", finalI))
            );
        }
    }

    @Test
    void term() {
        long offset = 100;
        long num = 100;
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(new Snapshot(null, new SnapshotMetadata(offset, 1)));
        RaftLog log = RaftLog.newRaftLog(storage);
        for (long i = 1; i < num; i++) {
            log.append(List.of(new Entry(offset + i, i)));
        }
        class Test {
            long idx, wIdx;

            public Test(long idx, long wIdx) {
                this.idx = idx;
                this.wIdx = wIdx;
            }
        }
        Test[] tests = new Test[]{
                new Test(offset - 1, 0),
                new Test(offset, 1),
                new Test(offset + num / 2, num / 2),
                new Test(offset + num - 1, num - 1),
                new Test(offset + num, 0),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            assertEquals(test.wIdx, log.term(test.idx), String.format("#%d", i));
        }
    }

    @Test
    void slice() {
        class Test {
            long from, to, limit;
            List<Entry> wEntries;
            boolean wEx;

            public Test(long from, long to, long limit, List<Entry> wEntries, boolean wEx) {
                this.from = from;
                this.to = to;
                this.limit = limit;
                this.wEntries = wEntries;
                this.wEx = wEx;
            }
        }
        long offset = 100;
        long num = 100;
        long last = offset + num;
        long half = offset + num / 2;
        Entry halfE = new Entry(half, half);
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(new Snapshot(null, new SnapshotMetadata(offset)));
        for (long i = 1; i < num / 2; i++) {
            storage.append(List.of(new Entry(offset + i, offset + i)));
        }
        RaftLog log = RaftLog.newRaftLog(storage);
        for (int i = (int) (num / 2); i < num; i++) {
            log.append(List.of(new Entry(offset + i, offset + i)));
        }
        Test[] tests = new Test[]{
                // test no limit
                new Test(offset - 1, offset + 1, UNLIMIT, null, false),
                new Test(offset, offset + 1, UNLIMIT, null, false),
                new Test(half - 1, half + 1, UNLIMIT,
                        List.of(new Entry(half - 1, half - 1), new Entry(half, half)), false),
                new Test(half, half + 1, UNLIMIT,
                        List.of(new Entry(half, half)), false),
                new Test(last - 1, last, UNLIMIT,
                        List.of(new Entry(last - 1, last - 1)), false),
                new Test(last, last + 1, UNLIMIT,
                        null, true),
                // test limit
                new Test(half - 1, half + 1, 0,
                        List.of(new Entry(half - 1, half - 1)), false),
                new Test(half - 1, half + 1, halfE.size() + 1,
                        List.of(new Entry(half - 1, half - 1)), false),
                new Test(half - 2, half + 1, halfE.size() + 1,
                        List.of(new Entry(half - 2, half - 2)), false),
                new Test(half - 1, half + 1, halfE.size() * 2,
                        List.of(new Entry(half - 1, half - 1), new Entry(half, half)), false),
                new Test(half - 1, half + 2, halfE.size() * 3,
                        List.of(new Entry(half - 1, half - 1), new Entry(half, half),
                                new Entry(half + 1, half + 1)), false),
                new Test(half, half + 2, halfE.size(),
                        List.of(new Entry(half, half)), false),
                new Test(half, half + 2, halfE.size() * 2,
                        List.of(new Entry(half, half), new Entry(half + 1, half + 1)), false),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            List<Entry> slice = new ArrayList<>();
            Executable testExec = () -> log.slice(test.from, test.to,
                    test.limit);
            if (test.wEx) {
                assertThrows(Throwable.class, testExec, "" + i);
                continue;
            }
            if (test.from <= offset) {
                assertThrows(CompactedException.class, testExec, "" + i);
            } else
                assertDoesNotThrow(() -> slice.addAll(log.slice(test.from, test.to,
                        test.limit)), "" + i);
            assertArrayEquals(test.wEntries == null ? new Entry[0] : test.wEntries.toArray(),
                    slice.toArray(), "" + i);
        }
    }

    @Test
    void checkOutOfBounds() {
        class Test {
            long lo, hi;
            boolean wEx, wCompactedEx;

            public Test(long lo, long hi, boolean wEx, boolean wCompactedEx) {
                this.lo = lo;
                this.hi = hi;
                this.wEx = wEx;
                this.wCompactedEx = wCompactedEx;
            }
        }
        long offset = 100;
        long num = 100;
        MemoryStorage storage = MemoryStorage.newMemoryStorage();
        storage.applySnapshot(new Snapshot(null, new SnapshotMetadata(offset)));
        RaftLog log = RaftLog.newRaftLog(storage);
        for (long i = 1; i <= num; i++) {
            log.append(List.of(new Entry(offset + i)));
        }
        long first = offset + 1;
        Test[] tests = new Test[]{
                new Test(first - 2, first + 1, false, true),
                new Test(first - 1, first + 1, false, true),
                new Test(first, first, false, false),
                new Test(first + num / 2, first + num / 2, false, false),
                new Test(first + num - 1, first + num - 1, false, false),
                new Test(first + num, first + num, false, false),
                new Test(first + num, first + num + 1, true, false),
                new Test(first + num + 1, first + num + 1, true, false),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            Executable testExec = () -> log.checkOutOfBounds(test.lo, test.hi);
            if (!test.wEx && !test.wCompactedEx)
                assertDoesNotThrow(testExec, "" + i);
            if (test.wCompactedEx)
                assertThrows(CompactedException.class, testExec, "" + i);
            if (test.wEx)
                assertThrows(Throwable.class, testExec, "" + i);
        }
    }

    @Test
    void stableTo() {
        class Test {
            long index, term, wOffset;

            public Test(long index, long term, long wOffset) {
                this.index = index;
                this.term = term;
                this.wOffset = wOffset;
            }
        }
        Test[] tests = new Test[]{
                new Test(1, 1, 2),
                new Test(2, 2, 3),
                new Test(2, 1, 1),// bad term
                new Test(3, 1, 1), // bad index
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            RaftLog raftLog = RaftLog.newRaftLog(MemoryStorage.newMemoryStorage());
            raftLog.append(List.of(new Entry(1, 1), new Entry(2, 2)));
            raftLog.stableTo(test.index, test.term);
            assertEquals(test.wOffset, raftLog.getUnstable().getOffset(), "" + i);
        }

    }
}