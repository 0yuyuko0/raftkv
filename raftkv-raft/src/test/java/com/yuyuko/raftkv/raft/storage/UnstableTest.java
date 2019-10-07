package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Entry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UnstableTest {

    @Test
    void truncateAndAppend() {
        class Test {
            List<Entry> entries;
            long offset;
            Snapshot snapshot;
            List<Entry> toAppends;

            long wOffset;
            List<Entry> wEntries;

            public Test(List<Entry> entries, long offset, Snapshot snapshot,
                        List<Entry> toAppends, long wOffset, List<Entry> wEntries) {
                this.entries = entries;
                this.offset = offset;
                this.snapshot = snapshot;
                this.toAppends = toAppends;
                this.wOffset = wOffset;
                this.wEntries = wEntries;
            }
        }
        Test[] tests = new Test[]{
                new Test(List.of(new Entry(5, 1)), 5, null,
                        List.of(new Entry(6, 1), new Entry(7, 1)), 5,
                        List.of(new Entry(5, 1), new Entry(6, 1), new Entry(7, 1))),
                new Test(List.of(new Entry(5, 1)), 5, null,
                        List.of(new Entry(5, 2), new Entry(6, 2)), 5,
                        List.of(new Entry(5, 2), new Entry(6, 2))),
                new Test(List.of(new Entry(5, 1)), 5, null,
                        List.of(new Entry(4, 2), new Entry(5, 2), new Entry(6, 2)),
                        4,
                        List.of(new Entry(4, 2), new Entry(5, 2), new Entry(6, 2))),
                new Test(List.of(new Entry(5, 1), new Entry(6, 1), new Entry(7, 1)),
                        5, null,
                        List.of(new Entry(6, 2)), 5,
                        List.of(new Entry(5, 1), new Entry(6, 2))),
                new Test(List.of(new Entry(5, 1), new Entry(6, 1), new Entry(7, 1)),
                        5, null,
                        List.of(new Entry(7, 2), new Entry(8, 2)), 5,
                        List.of(new Entry(5, 1), new Entry(6, 1), new Entry(7, 2),
                                new Entry(8, 2))),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            Unstable unstable = new Unstable(test.snapshot, test.entries, test.offset);
            unstable.truncateAndAppend(test.toAppends);
            int finalI = i;
            assertAll(
                    () -> assertEquals(test.wOffset, unstable.getOffset(), String.format("#%d: " +
                            "offset =" +
                            " %d, want %d", finalI, unstable.getOffset(), test.wOffset)),
                    () -> assertArrayEquals(test.wEntries.toArray(),
                            unstable.getEntries().toArray(),
                            String.format("#%d: \nwEnts = %s, \nwant %s", finalI,
                                    Arrays.toString(test.wEntries.toArray()),
                                    Arrays.toString(unstable.getEntries().toArray()
                                    )))
            );
        }
    }

    @Test
    void stableTo() {
        class Test {
            List<Entry> entries;
            long offset;
            Snapshot snapshot;
            long idx, term, wOffset, wLen;

            public Test(List<Entry> entries, long offset, Snapshot snapshot, long idx, long term,
                        long wOffset, long wLen) {
                this.entries = entries;
                this.offset = offset;
                this.snapshot = snapshot;
                this.idx = idx;
                this.term = term;
                this.wOffset = wOffset;
                this.wLen = wLen;
            }
        }
        Test[] tests = new Test[]{
                new Test(List.of(), 0, null, 5, 1, 0, 0),
                // stable to the first entry
                new Test(List.of(new Entry(5, 1)), 5, null,
                        5, 1, 6, 0),
                new Test(List.of(new Entry(5, 1), new Entry(6, 1)), 5, null,
                        5, 1, 6, 1),
                // stable to the first entry and term mismatch
                new Test(List.of(new Entry(6, 2)), 6, null,
                        6, 1, 6, 1),
                //// stable to old entry
                new Test(List.of(new Entry(5, 2)), 5, null,
                        4, 1, 5, 1),
                // stable to old entry
                new Test(List.of(new Entry(5, 1)), 5, null,
                        4, 2, 5, 1),
                // with snapshot
                // stable to the first entry
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null, new SnapshotMetadata(4,
                        1)),
                        5, 1, 6, 0),
                new Test(List.of(new Entry(5, 1), new Entry(6, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(4,
                                1)),
                        5, 1, 6, 1),
                new Test(List.of(new Entry(6, 2)), 6, new Snapshot(null, new SnapshotMetadata(5,
                        1)),// stable to the first entry and term mismatch
                        6, 1, 6, 1),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null, new SnapshotMetadata(4,
                        1)),// stable to snapshot
                        4, 1, 5, 1),
                new Test(List.of(new Entry(5, 2)), 5, new Snapshot(null, new SnapshotMetadata(4,
                        2)),// stable to old entry
                        4, 1, 5, 1),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            Unstable unstable = new Unstable(test.snapshot, test.entries, test.offset);
            unstable.stableTo(test.idx, test.term);
            int finalI = i;
            assertAll(
                    () -> assertEquals(test.wOffset, unstable.getOffset(), String.format("#%d: " +
                            "offset = %d, want %d", finalI, unstable.getOffset(), test.wOffset)),
                    () -> assertEquals(test.wLen, unstable.getEntries().size(), String.format(
                            "#%d" +
                            ": len = %d, want %d", finalI, unstable.getEntries().size(), test.wLen))
            );
        }
    }

    @Test
    void maybeFirstIndex() {
        class Test {
            List<Entry> entries;
            long offset;
            Snapshot snapshot;

            long wIdx;

            public Test(List<Entry> entries, long offset, Snapshot snapshot, long wIdx) {
                this.entries = entries;
                this.offset = offset;
                this.snapshot = snapshot;
                this.wIdx = wIdx;
            }
        }
        Test[] tests = new Test[]{
                new Test(List.of(new Entry(5, 1)), 5, null, 0),
                new Test(List.of(), 0, null, 0),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 5),
                new Test(List.of(), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 5),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            Unstable unstable = new Unstable(test.snapshot, test.entries, test.offset);
            assertEquals(test.wIdx, unstable.maybeFirstIndex(), String.format("#%d: index = %d, " +
                    "want %d", i, unstable.maybeFirstIndex(), test.wIdx));
        }
    }

    @Test
    void maybeLastIndex() {
        class Test {
            List<Entry> entries;
            long offset;
            Snapshot snapshot;

            long wIdx;

            public Test(List<Entry> entries, long offset, Snapshot snapshot, long wIdx) {
                this.entries = entries;
                this.offset = offset;
                this.snapshot = snapshot;
                this.wIdx = wIdx;
            }
        }
        Test[] tests = new Test[]{
                // last in wEnts
                new Test(List.of(new Entry(5, 1)), 5, null, 5),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 5),
                // last in snapshot
                new Test(List.of(), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 4),
                new Test(List.of(), 0, null, 0),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            Unstable unstable = new Unstable(test.snapshot, test.entries, test.offset);
            assertEquals(test.wIdx, unstable.maybeLastIndex(), String.format("#%d: index = %d, " +
                    "want %d", i, unstable.maybeLastIndex(), test.wIdx));
        }
    }

    @Test
    void maybeTerm() {
        class Test {
            List<Entry> entries;
            long offset;
            Snapshot snapshot;
            long index;

            long wTerm;

            public Test(List<Entry> entries, long offset, Snapshot snapshot, long index,
                        long wTerm) {
                this.entries = entries;
                this.offset = offset;
                this.snapshot = snapshot;
                this.index = index;
                this.wTerm = wTerm;
            }
        }
        Test[] tests = new Test[]{
                // last in wEnts
                new Test(List.of(new Entry(5, 1)), 5, null, 5, 1),
                new Test(List.of(new Entry(5, 1)), 5, null, 6, 0),
                new Test(List.of(new Entry(5, 1)), 5, null, 4, 0),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 5, 1),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 6, 0),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 4, 1),
                new Test(List.of(new Entry(5, 1)), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 3, 0),
                new Test(List.of(), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 5, 0),
                new Test(List.of(), 5, new Snapshot(null,
                        new SnapshotMetadata(new ConfState(), 4, 1)), 4, 1),
                new Test(List.of(), 5, null, 5, 0),
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            Unstable unstable = new Unstable(test.snapshot, test.entries, test.offset);
            assertEquals(test.wTerm, unstable.maybeTerm(test.index), String.format("#%d: term = " +
                    "%d, " +
                    "want %d", i, unstable.maybeTerm(test.index), test.wTerm));
        }
    }
}