package com.yuyuko.raftkv.raft.node;

import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.utils.concurrent.Chan;

/**
 * 代表Raft集群中的一个节点
 */
public interface Node {
    /**
     * 应用层每次tick时需要调用该函数，将会由这里驱动raft的一些操作比如选举等。
     * 至于tick的单位是多少由应用层自己决定，只要保证是恒定时间都会来调用一次就好了
     */
    void tick();

    /**
     * 调用该函数将驱动节点进入候选人状态，进而将竞争leader
     */
    void campaign();

    /**
     * 提议写入数据到日志中，可能会抛异常
     */
    void propose(byte[] data);

    /**
     * 提交日志变更
     */
    void proposeConfChange(ConfChange cc);

    /**
     * 将消息加入状态机执行
     *
     * @param msg 执行的消息
     */
    void step(Message msg);

    /**
     * 这里是核心函数，将返回Ready的queue，应用层需要关注这个queue，当发生变更时将其中的数据进行操作
     *
     * @return Ready的阻塞队列
     */
    Chan<Ready> ready();

    /**
     * Advance函数是当使用者已经将上一次Ready数据处理之后，调用该函数告诉raft库可以进行下一步的操作
     */
    void advance();

    /**
     * 一致性读相关
     *
     * @param readCtx 读请求id
     */
    void readIndex(byte[] readCtx);

    /**
     * 提交配置变更
     * @param confChange 配置
     * @return 当前配置状态
     */
    ConfState applyConfChange(ConfChange confChange);

    Status status();

    void stop();

    enum NodeState {
        Follower,
        Candidate,
        Leader,
    }
}
