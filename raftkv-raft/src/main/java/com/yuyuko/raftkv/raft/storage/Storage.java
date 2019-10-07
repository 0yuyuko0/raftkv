package com.yuyuko.raftkv.raft.storage;

import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.core.HardState;
import com.yuyuko.raftkv.raft.utils.Tuple;

import java.util.List;

/**
 * 提供了持久化日志的相关接口
 */
public interface Storage {
    /**
     *
     * @return 返回保存的初始状态
     */
    Tuple<HardState, ConfState> initialState();

    /**
     *
     * @param lo 上界
     * @param hi 下界
     * @param maxSize 大小
     * @return 返回索引范围在[lo,hi)之内并且不大于maxSize的entries数组
     */
    List<Entry> entries(long lo, long hi, long maxSize);

    /**
     * @param index 索引值
     * @return 这个索引值对应的任期号
     */
    long term(long index);

    /**
     * 返回第一条数据的索引值 + 1
     *
     * @return 第一条数据的索引值 + 1
     */
    long firstIndex();

    /**
     * 返回最后一条数据的索引值
     *
     * @return 最后一条数据的索引值
     */
    long lastIndex();

    /**
     * @return 返回最新的快照数据
     */
    Snapshot snapshot();
}
