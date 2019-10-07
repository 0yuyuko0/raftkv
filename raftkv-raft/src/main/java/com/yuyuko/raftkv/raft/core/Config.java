package com.yuyuko.raftkv.raft.core;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.storage.Storage;

import java.util.List;

public class Config {
    /**
     * 节点id
     */
    private long id;

    /**
     * 所有节点id的数组
     */
    private List<Long> peers;

    /**
     * 选举超时的tick
     */
    private int electionTick;

    /**
     * 心跳超时的tick
     */
    private int heartbeatTick;

    /**
     * 存储接口
     */
    private Storage storage;

    /**
     * 最后一个被应用到状态机的index
     * 重启raft时指定
     */
    private long applied;

    /**
     * 每个消息最大的大小
     */
    private long maxSizePerMsg;
    /**
     * 记leader是否需要检查集群中超过半数节点的活跃性，如果在选举超时内没有满足该条件，leader切换到follower状态
     */
    private boolean checkQuorum;

    public void validate() {
        if (id == 0)
            throw new RaftException("cannot use none as id");
        if (heartbeatTick <= 0)
            throw new RaftException("heartbeat tick must be greater than 0");
        if (electionTick <= heartbeatTick)
            throw new RaftException("election tick must be greater than heartbeat tick");
        if (storage == null)
            throw new RaftException("storage cannot be null");
    }

    public long getId() {
        return id;
    }

    public List<Long> getPeers() {
        return peers;
    }

    public int getElectionTick() {
        return electionTick;
    }

    public int getHeartbeatTick() {
        return heartbeatTick;
    }

    public Storage getStorage() {
        return storage;
    }

    public long getApplied() {
        return applied;
    }

    public long getMaxSizePerMsg() {
        return maxSizePerMsg;
    }

    public boolean isCheckQuorum() {
        return checkQuorum;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setPeers(List<Long> peers) {
        this.peers = peers;
    }

    public void setElectionTick(int electionTick) {
        this.electionTick = electionTick;
    }

    public void setHeartbeatTick(int heartbeatTick) {
        this.heartbeatTick = heartbeatTick;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public void setApplied(long applied) {
        this.applied = applied;
    }

    public void setMaxSizePerMsg(long maxSizePerMsg) {
        this.maxSizePerMsg = maxSizePerMsg;
    }

    public void setCheckQuorum(boolean checkQuorum) {
        this.checkQuorum = checkQuorum;
    }
}
