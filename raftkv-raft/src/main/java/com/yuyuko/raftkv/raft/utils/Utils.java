package com.yuyuko.raftkv.raft.utils;

import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.core.HardState;
import com.yuyuko.raftkv.raft.storage.Snapshot;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Utils {
    public static List<Entry> limitSize(List<Entry> entries, long maxSize) {
        if (Utils.isEmpty(entries))
            return new ArrayList<>();
        int i;
        long size = entries.get(0).size();
        for (i = 1; i < entries.size(); i++) {
            size += entries.get(i).size();
            if (size > maxSize)
                break;
        }
        return new ArrayList<>(entries.subList(0, i));
    }

    public static <T> boolean isEmpty(Collection<T> collection) {
        return collection == null || collection.size() == 0;
    }

    public static <T> boolean notEmpty(Collection<T> collection) {
        return !isEmpty(collection);
    }

    public static boolean isEmptySnapshot(Snapshot snapshot) {
        return snapshot == null || snapshot.getMetadata().getIndex() == 0;
    }

    public static boolean isCollectionEquals(Collection a, Collection b) {
        if (isEmpty(a) && isEmpty(b))
            return true;
        if (isEmpty(a) || isEmpty(b))
            return false;
        return a.equals(b);
    }

    public static boolean isEmptyHardState(HardState hardState) {
        if (hardState == null)
            return true;
        return HardState.EMPTY.equals(hardState);
    }
}
