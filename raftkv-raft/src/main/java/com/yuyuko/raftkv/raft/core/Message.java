package com.yuyuko.raftkv.raft.core;

import com.yuyuko.raftkv.raft.storage.Snapshot;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 在entries被写入持久化存储中以后，需要发送给节点的数据。
 * 字段都是可选的，在MessageType中有说明
 */
public class Message {

    /**
     * 消息类型，每条消息中都存在的字段
     */
    private MessageType type;

    /**
     * 接受消息节点id，每条消息中都存在的字段
     */
    private long from;

    /**
     * 发送消息节点id，每条消息中都存在的字段
     */
    private long to;

    /**
     * 当前节点的任期
     */
    private long term;

    /**
     * 发送的日志所处的任期
     */
    private long logTerm;

    /**
     * 日志索引id
     */
    private long index;

    /**
     * 日志条目数组
     */
    private List<Entry> entries;

    /**
     * 需要提交日志的index
     */
    private long commit;

    /**
     * 快照数据
     */
    private Snapshot snapShot;

    /**
     * 是否拒绝
     */
    private boolean reject;

    /**
     * 拒绝同步日志请求时返回的当前节点日志ID，用于被拒绝方快速定位到下一次合适的同步日志位置
     */
    private long rejectHint;

    /**
     * 上下文数据
     */
    private byte[] context;


    public static MessageBuilder builder() {
        return new MessageBuilder();
    }

    public static class MessageBuilder {
        private Message message = new Message();

        public MessageBuilder type(MessageType type) {
            message.type = type;
            return this;
        }

        public MessageBuilder from(long from) {
            message.from = from;
            return this;
        }

        public MessageBuilder to(long to) {
            message.to = to;
            return this;
        }

        public MessageBuilder snapshot(Snapshot snapshot) {
            message.snapShot = snapshot;
            return this;
        }

        public MessageBuilder reject(boolean reject) {
            message.reject = reject;
            return this;
        }

        public MessageBuilder commit(long commit) {
            message.commit = commit;
            return this;
        }

        public MessageBuilder context(byte[] context) {
            message.context = context;
            return this;
        }

        public Message build() {
            return message;
        }

        public MessageBuilder index(long index) {
            message.index = index;
            return this;
        }

        public MessageBuilder logTerm(long logTerm) {
            message.logTerm = logTerm;
            return this;
        }

        public MessageBuilder entries(List<Entry> entries) {
            message.entries = entries;
            return this;
        }

        public MessageBuilder rejectHint(long rejectHint) {
            message.rejectHint = rejectHint;
            return this;
        }

        public MessageBuilder term(long term) {
            message.term = term;
            return this;
        }
    }

    public enum MessageType {
        /**
         * 不用于节点间通信，仅用于发送给本节点让本节点进行选举
         */
        MsgHup,
        /**
         * 不用于节点间通信，仅用于leader节点在heartbeat定时器到期时向集群中其他节点发送心跳消息
         */
        MsgBeat,
        /**
         * 用户向raft提交数据
         *
         * @see #entries
         * 对于Candidate节点：忽略该消息，candidate节点没有处理propose数据的责任
         * 对于Follower节点：首先会检查集群内是否有leader存在，如果当前没有leader存在说明还在选举过程中，
         * 这种情况忽略这类消息；否则转发给leader处理。
         * 对于Leader节点：
         * 1.检查entries数组是否没有数据，这是一个保护性检查。
         * 2.检查本节点是否还在集群之中，如果已经不在了则直接返回不进行下一步处理。什么情况下会出现一个leader
         * 节点发现自己不存在集群之中了？这种情况出现在本节点已经通过配置变化被移除出了集群的场景。
         * 3.检查raft.leadTransferee字段，当这个字段不为0时说明正在进行leader迁移操作，
         * 这种情况下不允许提交数据变更操作，因此此时也是直接返回的。
         * 4. 检查消息的entries数组，看其中是否带有配置变更的数据。如果其中带有数据变更而raft
         * .pendingConf为true，说明当前有未提交的配置更操作数据，根据raft论文，每次不同同时进行一次以上的配置变更，
         * 因此这里会将entries
         * 数组中的配置变更数据置为空数据。
         * 5.到了这里可以进行真正的数据propose操作了，将调用raft算法库的日志模块写入数据，根据返回的情况向其他节点广播消息。
         */
        MsgProp,
        /**
         * leader向集群中其他节点同步数据
         *
         * @see #entries
         * @see #logTerm
         * @see #index
         */
        MsgApp,
        /**
         * 在节点收到leader的MsgApp/MsgSnap消息时，可能出现leader上的数据与自身节点数据不一致的情况，
         * 这种情况下会返回reject为true的MsgAppResp消息，同时rejectHint字段是本节点raft最后一条日志的索引ID。
         * index字段则返回的是当前节点的日志索引ID，用于向leader汇报自己已经commit的日志数据ID，
         * 这样leader就知道下一次同步数据给这个节点时，从哪条日志数据继续同步了。
         * 1.如果msg.Reject为true，说明节点拒绝了前面的MsgApp/MsgSnap消息，根据msg
         * RejectHint成员回退leader上保存的关于该节点的日志记录状态。
         * a.  因为上面节点拒绝了这次数据同步，所以节点的状态可能存在一些异常，此时如果leader上保存的节点状态为
         * ProgressStateReplicate，那么将切换到ProgressStateProbe状态。
         * b.  前面已经按照msg.RejectHint修改了leader上关于该节点日志状态的索引数据，
         * 接着再次尝试按照这个新的索引数据向该节点再次同步数据。
         * 2.如果msg.Reject为false。这种情况说明这个节点通过了leader的这一次数据同步请求，这种情况下根据msg.Index
         * 来判断在leader中保存的该节点日志数据索引是否发生了更新，如果发生了更新那么就说明这个节点通过了新的数据
         *
         * @see #index
         * @see #reject
         * @see #rejectHint
         */
        MsgAppResp,
        /**
         * 节点投票给自己以进行新一轮的选举
         *
         * @see #term
         * @see #index
         * @see #logTerm
         * @see #context
         */
        MsgVote,
        /**
         * 投票应答消息
         *
         * @see #reject
         */
        MsgVoteResp,
        /**
         * leader向集群中其他节点同步快照数据
         * 在某些时刻，leader节点会将日志数据进行压缩处理。
         * 而压缩后的快照数据实际上已经没有日志索引相关的信息了。这时候只能将快照数据全部同步给节点了
         *
         * @see #snapShot
         */
        MsgSnap,
        /**
         * 用于leader向follower发送心跳消息
         *
         * @see #commit
         * @see #context 在这里保存一致性读相关的数据
         */
        MsgHeartbeat,
        /**
         * 用于follower向leader应答心跳消息
         *
         * @see #context 在这里保存一致性读相关的数据
         */
        MsgHeartbeatResp,
        /**
         * 用于应用层向raft库汇报某个节点当前已不可达
         * 仅leader才处理这类消息，leader如果判断该节点此时处于正常接收数据的状态（ProgressStateReplicate），
         * 那么就切换到探测状态
         */
        MsgUnreachable,
        /**
         * 用于应用层向raft库汇报某个节点当前接收快照状态
         * 仅leader处理这类消息：
         * <p>
         * 1.如果reject为false：表示接收快照成功，将切换该节点状态到探测状态。
         * 2.否则接收失败。
         *
         * @see #reject
         */
        MsgSnapStatus,
        /**
         * 用于leader检查集群可用性的消息
         * 在超过选举时间时，如果当前打开了raft.checkQuorum开关，那么leader将给自己发送一条MsgCheckQuorum消息，
         * 对该消息的处理是：检查集群中所有节点的状态，如果超过半数的节点都不活跃了，那么leader也切换到follower状态。
         */
        MsgCheckQuorum,
        /**
         * 用于一致性读
         *
         * @see #entries
         */
        MsgReadIndex,
        /**
         * 用于一致性读
         */
        MsgReadIndexResp,

    }

    public static boolean isResponseMsg(MessageType type){
        return type == MessageType.MsgAppResp || type == MessageType.MsgVoteResp
                || type == MessageType.MsgHeartbeatResp || type == MessageType.MsgUnreachable;
    }

    public static boolean isLocalMsg(MessageType type){
        return type == MessageType.MsgHup || type == MessageType.MsgBeat
                || type == MessageType.MsgUnreachable || type == MessageType.MsgSnapStatus
                || type == MessageType.MsgCheckQuorum;
    }


    public MessageType getType() {
        return type;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public long getTerm() {
        return term;
    }

    public long getLogTerm() {
        return logTerm;
    }

    public long getIndex() {
        return index;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public long getCommit() {
        return commit;
    }

    public Snapshot getSnapShot() {
        return snapShot;
    }

    public boolean isReject() {
        return reject;
    }

    public long getRejectHint() {
        return rejectHint;
    }

    public byte[] getContext() {
        return context;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public void setLogTerm(long logTerm) {
        this.logTerm = logTerm;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public void setCommit(long commit) {
        this.commit = commit;
    }

    public void setSnapShot(Snapshot snapShot) {
        this.snapShot = snapShot;
    }

    public void setReject(boolean reject) {
        this.reject = reject;
    }

    public void setRejectHint(long rejectHint) {
        this.rejectHint = rejectHint;
    }

    public void setContext(byte[] context) {
        this.context = context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return from == message.from &&
                to == message.to &&
                term == message.term &&
                logTerm == message.logTerm &&
                index == message.index &&
                commit == message.commit &&
                reject == message.reject &&
                rejectHint == message.rejectHint &&
                type == message.type &&
                Objects.equals(entries, message.entries) &&
                Objects.equals(snapShot, message.snapShot) &&
                Arrays.equals(context, message.context);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(type, from, to, term, logTerm, index, entries, commit, snapShot
                , reject, rejectHint);
        result = 31 * result + Arrays.hashCode(context);
        return result;
    }

    @Override
    public String toString() {
        return "Message{" +
                "type=" + type +
                ", from=" + from +
                ", to=" + to +
                ", term=" + term +
                ", logTerm=" + logTerm +
                ", index=" + index +
                ", entries=" + entries +
                ", commit=" + commit +
                ", snapShot=" + snapShot +
                ", reject=" + reject +
                ", rejectHint=" + rejectHint +
                ", context=" + Arrays.toString(context) +
                '}';
    }
}