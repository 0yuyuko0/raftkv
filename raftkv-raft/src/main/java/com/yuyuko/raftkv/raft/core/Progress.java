package com.yuyuko.raftkv.raft.core;

/**
 * 该数据结构用于在leader中保存每个follower的状态信息，leader将根据这些信息决定发送给节点的日志
 */
public class Progress {
    /**
     * Next保存的是下一次leader发送append消息时传送过来的日志索引
     * 当选举出新的leader时，首先初始化Next为该leader最后一条日志+1
     * 如果向该节点append日志失败，则递减Next回退日志，一直回退到索引匹配为止
     */
    private long next;

    /**
     * Match保存在该节点上保存的日志的最大索引，初始化为0
     * 正常情况下，Next = Match + 1
     * 以下情况下不是上面这种情况：
     * 1. 切换到Probe状态时，如果上一个状态是Snapshot状态，即正在接收快照，
     * 那么Next = max(pr.Match+1, pendingSnapshot+1)
     * 2. 当该follower不在Replicate状态时，说明不是正常的接收副本状态。
     * 此时当leader与follower同步leader上的日志时，可能出现覆盖的情况，
     * 即此时follower上面假设Match为3，但是索引为3的数据会被
     * leader覆盖，此时Next指针可能会一直回溯到与leader上日志匹配的位置，
     * 再开始正常同步日志，此时也会出现Next != Match + 1的情况出现
     */
    private long match;

    private ProgressState state = ProgressState.Replicate;

    /**
     * 在状态切换到Probe状态以后，该follower就标记为Paused，此时将暂停同步日志到该节点
     */
    private boolean paused;

    /**
     * 如果向该节点发送快照消息，PendingSnapshot用于保存快照消息的索引
     * 当PendingSnapshot不为0时，该节点也被标记为暂停状态。
     * raft只有在这个正在进行中的快照同步失败以后，才会重传快照消息
     */
    private long pendingSnapshot;

    /**
     * 标记该节点是否活跃
     */
    private boolean recentActive;

    public void becomeSnapshot(long snapIdx) {
        resetState(ProgressState.Snapshot);
        pendingSnapshot = snapIdx;
    }

    private void resetState(ProgressState state) {
        paused = false;
        pendingSnapshot = 0;
        this.state = state;
    }


    /**
     * maybeDecrTo函数在传入的索引不在范围内的情况下返回false
     * 否则将把该节点的index减少到min(rejected,last)然后返回true
     * rejected是拒绝该append消息时的索引，last是拒绝该消息的节点的最后一条日志索引
     *
     * @param rejected
     * @param last
     * @return
     */
    public boolean maybeDecrTo(long rejected, long last) {
        if (state == ProgressState.Replicate) {// 如果当前在接收副本状态
            if (rejected <= match) {
                // 这种情况说明返回的情况已经过期，中间有其他添加成功的情况，导致match索引递增，
                // 此时不需要回退索引，返回false
                return false;
            }
            next = match + 1;
            return true;
        }
        // 以下都不是接收副本状态的情况

        // 为什么这里不是对比Match？因为Next涉及到下一次给该Follower发送什么日志，
        // 所以这里对比和下面修改的是Next索引
        // Match只表示该节点上存放的最大日志索引，而当leader发生变化时，可能会覆盖一些日志
        if (next - 1 != rejected)
            // 这种情况说明返回的情况已经过期，不需要回退索引，返回false
            return false;

        // 到了这里就回退Next为两者的较小值
        if ((next = Math.min(rejected, last + 1)) < 1)
            next = 1;
        resume();
        return true;
    }

    public void optimisticUpdate(long n) {
        next = n + 1;
    }

    public void becomeReplicate() {
        resetState(ProgressState.Replicate);
        next = match + 1;
    }


    /**
     *     // 可以中断快照的情况：当前为接收快照，同时match已经大于等于快照索引
     * // 因为match已经大于快照索引了，所以这部分快照数据可以不接收了，也就是可以被中断的快照操作
     * // 因为在节点落后leader数据很多的情况下，可能leader会多次通过snapshot同步数据给节点，
     * // 而当 pr.Match >= pr.PendingSnapshot的时候，说明通过快照来同步数据的流程完成了，这时可以进入正常的接收同步数据状态了。
     * @return 是否可以中断快照
     */
    public boolean needSnapshotAbort() {
        return state == ProgressState.Snapshot && match >= pendingSnapshot;
    }

    public void snapshotFailure() {
        pendingSnapshot = 0;
    }

    /**
     * 修改为probe状态
     */
    public void becomeProbe() {
        if(state == ProgressState.Snapshot){
            long pendingSnapshot = this.pendingSnapshot;
            resetState(ProgressState.Probe);
            next = Math.max(match + 1, pendingSnapshot + 1);
        }else{
            resetState(ProgressState.Probe);
            next = match + 1;
        }
    }

    public enum ProgressState {
        /**
         * 在每次heartbeat消息间隔期最多发一条同步日志消息给该节点
         */
        Probe,
        /**
         * 正常的接受副本数据状态。当处于该状态时，leader在发送副本消息之后,
         * 就修改该节点的next索引为发送消息的最大索引+1
         */
        Replicate,
        /**
         * 接收快照状态
         */
        Snapshot
    }

    public Progress(){

    }

    public Progress(long next) {
        this.next = next;
    }

    public Progress(long next, long match) {
        this.next = next;
        this.match = match;
    }

    public void setMatch(long match) {
        this.match = match;
    }

    /**
     * 收到appresp的成功应答之后，leader更新节点的索引数据
     * 如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
     */
    public boolean maybeUpdate(long index) {
        boolean updated = false;
        if (match < index) {
            match = index;
            resume();
            updated = true;
        }
        if (next < index + 1)
            next = index + 1;

        return updated;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    public void setNext(long next) {
        this.next = next;
    }

    public void setState(ProgressState state) {
        this.state = state;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    public void setPendingSnapshot(long pendingSnapshot) {
        this.pendingSnapshot = pendingSnapshot;
    }

    public void setRecentActive(boolean recentActive) {
        this.recentActive = recentActive;
    }

    public long getNext() {
        return next;
    }

    public long getMatch() {
        return match;
    }

    public long getPendingSnapshot() {
        return pendingSnapshot;
    }

    public boolean isRecentActive() {
        return recentActive;
    }

    public ProgressState getState() {
        return state;
    }

    public boolean isPaused() {
        return paused;
    }

    @Override
    public String toString() {
        return "Progress{" +
                "next=" + next +
                ", match=" + match +
                ", state=" + state +
                ", paused=" + paused +
                ", pendingSnapshot=" + pendingSnapshot +
                ", recentActive=" + recentActive +
                '}';
    }
}
