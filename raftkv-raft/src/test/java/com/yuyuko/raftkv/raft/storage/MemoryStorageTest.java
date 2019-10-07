package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Entry;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MemoryStorageTest {
    @Test
    public void append() {
        List<Entry> entries = List.of(new Entry(3, 3), new Entry(4, 4), new Entry(5, 5));
        class Test {
            List<Entry> entries, wEntries;
            Class<? extends Exception> wEx;

            public Test(List<Entry> entries, Class<? extends Exception> wEx, List<Entry> wEntries) {
                this.entries = entries;
                this.wEx = wEx;
                this.wEntries = wEntries;
            }
        }
        Test[] tests = new Test[]{
                new Test(List.of(new Entry(3, 3), new Entry(4, 4), new Entry(5, 5)),
                        null,
                        List.of(new Entry(3, 3), new Entry(4, 4), new Entry(5, 5))),
                new Test(List.of(new Entry(3, 3), new Entry(4, 6), new Entry(5, 6)),
                        null,
                        List.of(new Entry(3, 3), new Entry(4, 6), new Entry(5, 6))),
                new Test(List.of(new Entry(3, 3), new Entry(4, 4), new Entry(5, 5)
                        , new Entry(6, 5)),
                        null,
                        List.of(new Entry(3, 3), new Entry(4, 4), new Entry(5, 5),
                                new Entry(6, 5))),
                // truncate incoming wEnts, truncate the existing wEnts and append
                new Test(List.of(new Entry(2, 3), new Entry(3, 3), new Entry(4, 5)),
                        null,
                        List.of(new Entry(3, 3), new Entry(4, 5))),
                // truncate the existing wEnts and append
                new Test(List.of(new Entry(4, 5)),
                        null,
                        List.of(new Entry(3, 3), new Entry(4, 5))),
                // direct append
                new Test(List.of(new Entry(6, 5)),
                        null,
                        List.of(new Entry(3, 3), new Entry(4, 4), new Entry(5, 5),
                                new Entry(6, 5)))
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            MemoryStorage memoryStorage = new MemoryStorage(entries);
            Executable execTest = () -> memoryStorage.append(test.entries);
            if (test.wEx != null)
                assertThrows(test.wEx, execTest, String.format("#%d: want err %s", i, test.wEx));
            else
                assertAll(execTest);
            assertArrayEquals(test.wEntries.toArray(), memoryStorage.getEntries().toArray(),
                    String.format("\n#%d: wEnts = %s,\n want %s", i,
                            Arrays.toString(memoryStorage.getEntries().toArray()),
                            Arrays.toString(test.wEntries.toArray())));
        }
    }

    @Test
    public void entries() {
        class Test {
            long lo, hi, maxSize;
            Class<? extends Throwable> wEx;
            List<Entry> wEntries;

            public Test(long lo, long hi, long maxSize, Class<? extends Throwable> wEx,
                        List<Entry> wEntries) {
                this.lo = lo;
                this.hi = hi;
                this.maxSize = maxSize;
                this.wEx = wEx;
                this.wEntries = wEntries;
            }
        }
        List<Entry> entries = List.of(new Entry(3, 3), new Entry(4, 4),
                new Entry(5, 5), new Entry(6, 6));
        Test[] tests = new Test[]{
                new Test(2, 6, Integer.MAX_VALUE, CompactedException.class, null),
                new Test(3, 4, Integer.MAX_VALUE, CompactedException.class, null),
                new Test(4, 5, Integer.MAX_VALUE, null, List.of(new Entry(4, 4
                ))),
                new Test(4, 6, Integer.MAX_VALUE, null, List.of(new Entry(4, 4),
                        new Entry(5, 5))),
                new Test(4, 7, Integer.MAX_VALUE, null, List.of(new Entry(4, 4),
                        new Entry(5, 5), new Entry(6, 6))),
                new Test(4, 7, 0, null, List.of(new Entry(4, 4))),
                new Test(4, 7,
                        (entries.get(1).size() + entries.get(2).size() + entries.get(3).size() / 2),
                        null,
                        List.of(new Entry(4, 4),
                                new Entry(5, 5))),
                new Test(4, 7,
                        entries.get(1).size() + entries.get(2).size() + entries.get(3).size() - 1,
                        null,
                        List.of(new Entry(4, 4),
                                new Entry(5, 5))),
                new Test(4, 7,
                        entries.get(1).size() + entries.get(2).size() + entries.get(3).size(),
                        null,
                        List.of(new Entry(4, 4),
                                new Entry(5, 5),
                                new Entry(6, 6))
                )
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            MemoryStorage memoryStorage = new MemoryStorage(entries);
            int finalI = i;
            Executable testExec = () -> {
                List<Entry> res = memoryStorage.entries(test.lo,
                        test.hi,
                        test.maxSize);
                assertArrayEquals(test.wEntries.toArray(), res.toArray(),
                        String.format("\n #%d: wEnts = %s, \n want %s", finalI,
                                Arrays.toString(res.toArray()),
                                Arrays.toString(test.wEntries.toArray())));
            };
            if (test.wEx != null) {
                assertThrows(test.wEx, testExec, String.format("#%d: want err %s", i, test.wEx));
            } else
                assertDoesNotThrow(testExec);
        }
    }


    @Test
    public void term() {
        class Test {
            long index, term;
            Class<? extends Exception> wEx;

            public Test(long index, long term, Class<? extends Exception> wEx) {
                this.index = index;
                this.term = term;
                this.wEx = wEx;
            }
        }
        List<Entry> entries = List.of(new Entry(3, 3), new Entry(4, 4),
                new Entry(5, 5));
        Test[] tests = new Test[]{
                new Test(2, 0, CompactedException.class),
                new Test(3, 3, null),
                new Test(4, 4, null),
                new Test(5, 5, null),
                new Test(6, 6, UnavailableException.class),
        };
        for (Test test : tests) {
            MemoryStorage memoryStorage = new MemoryStorage(entries);
            Executable execTest = () -> assertEquals(test.term, memoryStorage.term(test.index));
            if (test.wEx != null)
                assertThrows(test.wEx, execTest);
            else
                assertAll(execTest);
        }

    }

    @Test
    public void compact() {
        class Test {
            long compactIdx, wIdx, wTerm;
            int wLen;
            Class<? extends Exception> wEx;

            Test(long compactIdx, long wIdx, long wTerm, int wLen, Class<?
                    extends Exception> wEx) {
                this.compactIdx = compactIdx;
                this.wIdx = wIdx;
                this.wTerm = wTerm;
                this.wLen = wLen;
                this.wEx = wEx;
            }
        }
        List<Entry> entries = List.of(
                new Entry(3, 3),
                new Entry(4, 4),
                new Entry(5, 5));
        Test[] tests = new Test[]{
                new Test(2, 3, 3, 3, CompactedException.class),
                new Test(3, 3, 3, 3, CompactedException.class),
                new Test(4, 4, 4, 2, null),
                new Test(5, 5, 5, 1, null)
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            MemoryStorage storage = new MemoryStorage(entries);
            Executable testExec = () -> storage.compact(test.compactIdx);
            if (test.wEx != null)
                assertThrows(test.wEx, testExec);
            else
                assertDoesNotThrow(testExec);
            assertAll(
                    () -> assertEquals(test.wIdx, storage.getEntries().get(0).getIndex()),
                    () -> assertEquals(test.wTerm, storage.getEntries().get(0).getTerm()),
                    () -> assertEquals(test.wLen, storage.getEntries().size())
            );
        }
    }

    @Test
    public void createSnapshot() {
        class Test {
            long index;
            Snapshot wSnap;
            Class<? extends Exception> wEx;

            public Test(long index, Snapshot wSnap, Class<? extends Exception> wEx) {
                this.index = index;
                this.wSnap = wSnap;
                this.wEx = wEx;
            }
        }

        List<Entry> entries = List.of(new Entry(3, 3), new Entry(4, 4),
                new Entry(5, 5));
        ConfState cs = new ConfState(List.of(1L, 2L, 3L));
        byte[] data = "data".getBytes();

        Test[] tests = new Test[]{
                new Test(4, new Snapshot(data, new SnapshotMetadata(cs, 4, 4)), null),
                new Test(5, new Snapshot(data, new SnapshotMetadata(cs, 5, 5)), null)
        };
        for (Test test : tests) {
            MemoryStorage storage = new MemoryStorage(entries);
            assertEquals(test.wSnap, storage.createSnapshot(test.index, cs, data));
        }
    }

    @Test
    public void applySnapshot() {
        ConfState cs = new ConfState(List.of(1L, 2L, 3L));
        byte[] bytes = "data".getBytes();
        Snapshot[] snapshots = new Snapshot[]{
                new Snapshot(bytes, new SnapshotMetadata(cs, 4, 4)),
                new Snapshot(bytes, new SnapshotMetadata(cs, 3, 3))
        };
        MemoryStorage storage = MemoryStorage.newMemoryStorage();

        assertDoesNotThrow(() -> storage.applySnapshot(snapshots[0]));
        assertThrows(SnapOutOfDateException.class, () -> storage.applySnapshot(snapshots[1]));
    }

    @Test
    public void firstIndex() {
        List<Entry> entries = List.of(new Entry(3, 3), new Entry(4, 4),
                new Entry(5, 5));
        MemoryStorage storage = new MemoryStorage(entries);
        assertEquals(4, storage.firstIndex());
        storage.compact(4);
        assertEquals(5, storage.firstIndex());
    }

    @Test
    public void lastIndex() {
        List<Entry> entries = List.of(new Entry(3, 3), new Entry(4, 4),
                new Entry(5, 5));
        MemoryStorage storage = new MemoryStorage(entries);
        assertAll(
                () -> assertEquals(5, storage.lastIndex()),
                () -> {
                    storage.append(List.of(new Entry(6, 5)));
                    assertEquals(6, storage.lastIndex());
                }
        );
    }
}