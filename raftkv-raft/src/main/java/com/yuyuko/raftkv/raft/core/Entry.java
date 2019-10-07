package com.yuyuko.raftkv.raft.core;

import java.util.Arrays;
import java.util.Objects;

/**
 * 需要在消息发送之前被写入到持久化存储中的entries数据数组
 */
public class Entry {
    public enum EntryType {
        Normal,
        ConfChange,
    }

    private long term;

    private long index;

    private EntryType type;

    private byte[] data;

    public Entry(EntryType type) {
        this.type = type;
    }

    public Entry(EntryType type, byte[] data) {
        this.type = type;
        this.data = data;
    }

    public Entry(long index) {
        this(index, 0);
    }

    public Entry(long index, long term) {
        this(index, term, EntryType.Normal);
    }

    public Entry(long index, long term, EntryType type) {
        this(index, term, type, null);
    }

    public Entry(long index, long term, byte[] data) {
        this(index, term, EntryType.Normal, data);
    }

    public Entry(long index, long term, EntryType type, byte[] data) {
        this.term = term;
        this.index = index;
        this.type = type;
        this.data = data;
    }

    public Entry() {
        this(0, 0, ((byte[]) null));
    }

    public Entry(byte[] data) {
        this(0, 0, data);
    }

    public byte[] getData() {
        return data;
    }

    public long getIndex() {
        return index;
    }

    public long getTerm() {
        return term;
    }

    public EntryType getType() {
        return type;
    }

    public long size() {
        long ans = 0;
        ans += 1 + sovRaft(term);
        ans += 1 + sovRaft(index);
        if (data != null) {
            ans += 1 + data.length + sovRaft(data.length);
        }
        return ans;
    }

    private long sovRaft(long x) {
        long ans = 0;
        while (true) {
            ++ans;
            x >>>= 7;
            if (x == 0)
                return ans;
        }
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public void setIndex(long index) {
        this.index = index;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entry entry = (Entry) o;
        return term == entry.term &&
                index == entry.index &&
                type == entry.type &&
                Arrays.equals(data, entry.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, index, type);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "term=" + term +
                ", index=" + index +
                ", type=" + type +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
