package com.yuyuko.raftkv.raft.core;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.node.*;
import com.yuyuko.raftkv.raft.read.ReadIndexStatus;
import com.yuyuko.raftkv.raft.read.ReadOnly;
import com.yuyuko.raftkv.raft.storage.*;
import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.raft.utils.Tuple;
import com.yuyuko.raftkv.raft.utils.Utils;
import com.yuyuko.raftkv.raft.node.Node;
import com.yuyuko.raftkv.raft.storage.CompactedException;
import com.yuyuko.raftkv.raft.storage.RaftLog;
import com.yuyuko.raftkv.raft.storage.Snapshot;
import com.yuyuko.raftkv.raft.storage.SnapshotTemporarilyUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Raft {
    private static final Logger LOGGER = LoggerFactory.getLogger(Raft.class);

    public static final long NONE = 0;

    public static final long UNLIMIT = Long.MAX_VALUE;

    private long id;

    private long term;

    private long vote;

    private List<ReadState> readStates = new ArrayList<>();

    private RaftLog raftLog;

    private long maxMsgSize;

    private Map<Long, Progress> progresses = new HashMap<>();

    private Node.NodeState state;

    private List<Message> messages = new ArrayList<>();

    private Map<Long, Boolean> votes;

    private long lead;

    private ReadOnly readOnly;

    /**
     * 上一次选举超时过去的时间
     */
    private int electionElapsed;

    /**
     * 收到上一次心跳过去的时间
     */
    private int heartbeatElapsed;

    private boolean checkQuorum;

    private int electionTimeout;

    private int heartbeatTimeout;

    /**
     * 是一个在[electiontimeout, 2 * electiontimeout - 1]的随机值
     */
    private int randomizedElectionTimeout;

    private StepFunction stepFunc;

    private TickFunction tickFunc;


    /**
     * @param config 算法设置
     * @return Raft
     */
    public static Raft newRaft(Config config) {
        config.validate();
        Tuple<HardState, ConfState> hardStateConfStateTuple = config.getStorage().initialState();
        HardState hardState = hardStateConfStateTuple.getFirst();
        ConfState confState = hardStateConfStateTuple.getSecond();
        List<Long> peers = config.getPeers();
        if (peers == null)
            peers = List.of();
        if (Utils.notEmpty(confState.getNodes())) {
            if (peers.size() > 0) {
                // the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snapshot.
                throw new RaftException("cannot specify both newRaft(peers) and ConfState" +
                        ".Nodes)");
            }
            peers = confState.getNodes();
        }
        Raft raft = new Raft();
        raft.id = config.getId();
        raft.lead = NONE;
        raft.raftLog = RaftLog.newRaftLog(config.getStorage());
        raft.maxMsgSize = config.getMaxSizePerMsg();
        raft.electionTimeout = config.getElectionTick();
        raft.heartbeatTimeout = config.getHeartbeatTick();
        raft.checkQuorum = config.isCheckQuorum();
        raft.readOnly = new ReadOnly();
        for (Long peer : peers) {
            raft.progresses.put(peer, new Progress(1));
        }
        // 如果不是第一次启动而是从之前的数据进行恢复
        if (hardState != null) {
            raft.loadState(hardState);
        }
        if (config.getApplied() > 0) {
            raft.raftLog.appliedTo(config.getApplied());
        }
        raft.becomeFollower(raft.term, NONE);

        String peersStr =
                peers.stream().sorted().map(Object::toString).collect(Collectors.joining(","
                ));
        LOGGER.info("newRaft {} [peers: [{}], term: {}, commit: {}, applied: {}, lastIndex: {}, " +
                        "lastTerm: {}]"
                , raft.id, peersStr, raft.term, raft.raftLog.getCommitted(),
                raft.raftLog.getApplied(), raft.raftLog.lastIndex(), raft.raftLog.lastTerm());
        return raft;
    }

    /**
     * raft状态机
     *
     * @param m 接收到的消息
     */
    public void step(Message m) {
        LOGGER.trace("from:{},to:{},type:{},term:{},state:{}", m.getFrom(), m.getTo(), m.getType(),
                term, state);
        //来自本地的消息
        if (m.getTerm() == 0)
            ;
        else if (m.getTerm() > term) {
            // 消息的Term大于节点当前的Term
            long lead = m.getFrom();
            // 如果收到的是投票类消息
            if (m.getType() == Message.MessageType.MsgVote) {
                // 是否在租约期以内
                boolean inLease =
                        checkQuorum && this.lead != NONE && electionElapsed < electionTimeout;
                //在租约期内可以忽略选举消息，见论文的4.2.3，这是为了阻止已经离开集群的节点再次发起投票请求
                if (inLease) {
                    LOGGER.info("{} [logterm: {}, index: {}, vote: {}] ignored {} from {} " +
                                    "[logterm: {}, index: {}] " +
                                    "at term {}: lease is not expired (remaining ticks: {})", id,
                            raftLog.lastTerm(), raftLog.lastIndex(), vote, m.getType(), m.getFrom(),
                            m.getLogTerm(), m.getIndex(), term, electionTimeout - electionElapsed);
                    return;
                }
                // 否则将lead置为空
                lead = NONE;
            }

            LOGGER.info("{} [term: {}] received a {} message with higher term from {} [term:" +
                            " {}]"
                    , id, term, m.getType(), m.getFrom(), m.getTerm());
            // 变成follower状态
            becomeFollower(m.getTerm(), lead);

        } else if (m.getTerm() < term) {
            // 消息的Term小于节点自身的Term，同时消息类型是心跳消息或者是append消息
            // 收到了一个节点发送过来的更小的term消息。这种情况可能是因为消息的网络延时导致，
            // 但是也可能因为该节点由于网络分区导致了它递增了term到一个新的任期。
            // ，这种情况下该节点不能赢得一次选举，也不能使用旧的任期号重新再加入集群中。
            // 如果checkQurom为false，这种情况可以使用递增任期号应答来处理。
            // 但是如果checkQurom为True，
            // 此时收到了一个更小的term的节点发出的HB或者APP消息，于是应答一个appresp消息，试图纠正它的状态
            if (checkQuorum && (m.getType() == Message.MessageType.MsgHeartbeat || m.getType() == Message.MessageType.MsgApp))
                send(Message.builder()
                        .to(m.getFrom())
                        .type(Message.MessageType.MsgAppResp).build()
                );
            else {
                // 除了上面的情况以外，忽略任何term小于当前节点所在任期号的消息
                LOGGER.info("{} [term: {}] ignored a {} message with lower term from {} [term: " +
                        "{}]", id, term, m.getType(), m.getFrom(), m.getTerm());
            }
            // 在消息的term小于当前节点的term时，不往下处理直接返回了
            return;
        }
        switch (m.getType()) {
            // 收到HUP消息，说明准备进行选举

            case MsgHup:
                if (state != Node.NodeState.Leader) {
                    LOGGER.info("{} is starting a new election at term {}", id, term);

                    // 进行选举
                    campaign();

                } else {
                    LOGGER.debug("{} ignoring MsgHup because already leader", id);
                }
                break;
            case MsgVote:
                // 收到投票类的消息
                if ((vote == NONE || vote == m.getFrom())
                        && raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {
                    // 如果当前没有给任何节点投票（r.Vote == None）或者投票的节点term大于本节点的（m.Term > r.Term）
                    // 或者是之前已经投票的节点（r.Vote == m.From）
                    // 同时还满足该节点的消息是最新的（r.raftLog.isUpToDate(m.Index, m.LogTerm)），那么就接收这个节点的投票
                    LOGGER.info("{} [logterm: {}, index: {}, vote: {}] cast {} for {} [logterm: " +
                                    "{}, index: {}] at term {}", id, raftLog.lastTerm(),
                            raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(),
                            m.getIndex(), m.getTerm());
                    send(Message.builder().
                            to(m.getFrom()).
                            type(Message.MessageType.MsgVoteResp).build());
                    // 保存下来给哪个节点投票了
                    electionElapsed = 0;
                    vote = m.getFrom();
                } else {
                    // 否则拒绝投票
                    LOGGER.info("{} [logterm: {}, index: {}, vote: {}] rejected {} from {} " +
                                    "[logterm: {}, index: {}] at term {}",
                            id, raftLog.lastTerm(),
                            raftLog.lastIndex(), vote, m.getType(), m.getFrom(), m.getLogTerm(),
                            m.getIndex(), m.getTerm());
                    send(Message.builder().
                            to(m.getFrom()).
                            type(Message.MessageType.MsgVoteResp).
                            reject(true).build());
                }
                break;
            default:
                // 其他情况下进入各种状态下自己定制的状态机函数
                stepFunc.step(this, m);
        }
    }

    private void campaign() {
        becomeCandidate();
        // 调用poll函数给自己投票，同时返回当前投票给本节点的节点数量
        if (quorum() == poll(id, Message.MessageType.MsgVoteResp, true)) {

            // 如果给自己投票之后，刚好超过半数的通过，那么就成为新的leader
            becomeLeader();
            return;
        }

        // 向集群里的其他节点发送投票消息
        progresses.forEach((id, p) -> {
            if (id == this.id)
                return;
            LOGGER.debug("{} [logterm: {}, index: {}] sent vote request to {} at term {}",
                    id, raftLog.lastTerm(), raftLog.lastIndex(), id, term);
            send(Message.builder()
                    .term(term)
                    .to(id)
                    .type(Message.MessageType.MsgVote)
                    .index(raftLog.lastIndex())
                    .logTerm(raftLog.lastTerm())
                    .build()
            );
        });
    }

    /**
     * 轮询集群中所有节点，返回一共有多少节点已经进行了投票
     *
     * @param id   投票id
     * @param type 投票类型
     * @param vote 是否投票
     * @return
     */
    private int poll(long id, Message.MessageType type, boolean vote) {
        if (vote) {
            if (id != this.id)
                LOGGER.info("{} received {} from {} at term {}", this.id, type, id, term);
        } else {
            LOGGER.info("{} received {} rejection from {} at term {}", this.id, type, id, term);
        }
        // 如果id没有投票过，那么更新id的投票情况
        votes.putIfAbsent(id, vote);
        return (int) votes.values().stream().filter(k -> k).count();
    }

    private int quorum() {
        return progresses.size() / 2 + 1;
    }

    private void send(Message m) {
        m.setFrom(id);
        if (m.getType() == Message.MessageType.MsgVote) {
            // 投票时term不能为空
            if (m.getTerm() == 0)
                throw new RaftException("term should be set when sending " + m.getType());
        } else {
            // 其他的消息类型，term必须为空
            if (m.getTerm() != 0) {
                throw new RaftException(String.format("term should not be set when sending %s " +
                        "(was %d)", m.getType(), m.getTerm()));
            }
            // prop消息和readindex消息不需要带上term参数
            if (m.getType() != Message.MessageType.MsgProp && m.getType() != Message.MessageType.MsgReadIndex)
                m.setTerm(term);
        }
        messages.add(m);
    }

    public void becomeFollower(long term, long lead) {
        stepFunc = stepFollower();
        reset(term);
        this.lead = lead;
        tickFunc = tickElection();
        state = Node.NodeState.Follower;
        LOGGER.info("{} became follower at term {}", id, term);
    }

    public void becomeCandidate() {
        if (state == Node.NodeState.Leader)
            throw new RaftException("invalid transition [leader -> candidate]");
        stepFunc = stepCandidate();
        // 因为进入candidate状态，意味着需要重新进行选举了，所以reset的时候传入的是Term+1
        reset(term + 1);
        tickFunc = tickElection();
        // 给自己投票
        vote = id;
        state = Node.NodeState.Candidate;
        LOGGER.info("{} became candidate at term {}", id, term);
    }

    public void becomeLeader() {
        if (state == Node.NodeState.Follower)
            throw new RaftException("invalid transition [follower -> leader]");
        stepFunc = stepLeader();
        reset(term);
        tickFunc = tickHeartBeat();
        lead = id;
        state = Node.NodeState.Leader;
        appendEntries(List.of(new Entry()));
        LOGGER.info("{} became leader at term {}", id, term);
    }

    public void appendEntries(List<Entry> entries) {
        long lastIndex = raftLog.lastIndex();
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            entry.setTerm(term);
            entry.setIndex(lastIndex + i + 1);
        }
        raftLog.append(entries);
        // 更新本节点的Next以及Match索引
        progresses.get(id).maybeUpdate(raftLog.lastIndex());
        // append之后，尝试一下是否可以进行commit
        maybeCommit();
    }

    /**
     * 尝试提交日志
     *
     * @return 是否提交成功
     */
    private boolean maybeCommit() {
        // 逆序排列
        long midCommittedIdx =
                this.progresses.values().stream().map(Progress::getMatch).sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList()).get(quorum() - 1);
        // 排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
        // 说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
        return raftLog.maybeCommit(midCommittedIdx, term);
    }

    /**
     * 重置raft的状态
     *
     * @param term term
     */
    public void reset(long term) {
        if (this.term != term) {
            // 如果是新的任期，那么保存任期号，同时将投票节点置空
            this.term = term;
            vote = NONE;
        }
        lead = NONE;

        electionElapsed = 0;
        heartbeatElapsed = 0;
        resetRandomizedElectionTimeout();

        votes = new HashMap<>();

        Map<Long, Progress> newProgresses = new HashMap<>();
        progresses.keySet().forEach(id -> {
            //所有节点的nextIndex置为最后一条commit的日志的index+1
            newProgresses.put(id, new Progress(raftLog.lastIndex() + 1));
            if (this.id == id)
                newProgresses.get(id).setMatch(raftLog.lastIndex());
        });

        progresses = newProgresses;
        readOnly = new ReadOnly();
    }

    private void resetRandomizedElectionTimeout() {
        randomizedElectionTimeout =
                electionTimeout + ThreadLocalRandom.current().nextInt(electionTimeout);
    }

    private boolean promotable() {
        return progresses.containsKey(id);
    }

    private boolean pastElectionTimeout() {
        return electionElapsed >= randomizedElectionTimeout;
    }

    private void loadState(HardState hardState) {
        RaftLog raftLog = this.raftLog;
        if (hardState.getCommit() < raftLog.getCommitted() || hardState.getCommit() > raftLog.lastIndex()) {
            LOGGER.error("{} hardState.commit {} is out of range [{}, {}]", id,
                    hardState.getCommit(), raftLog.getCommitted(), raftLog.lastIndex());
            throw new RaftException();
        }
        raftLog.setCommitted(hardState.getCommit());
        this.term = hardState.getTerm();
        this.vote = hardState.getVote();
    }

    private StepFunction stepLeader() {
        return (raft, m) -> {
            switch (m.getType()) {
                case MsgBeat:
                    // 广播HB消息
                    broadcastHeartbeat();
                    return;
                case MsgCheckQuorum:
                    // 检查集群可用性
                    if (!checkQuorumActive()) {
                        // 如果超过半数的服务器没有活跃
                        LOGGER.warn("{} stepped down to follower since quorum is not active", id);
                        becomeFollower(term, NONE);
                    }
                    return;
                case MsgProp:
                    // 不能提交空数据
                    if (Utils.isEmpty(m.getEntries())) {
                        LOGGER.error("{} stepped empty MsgProp", id);
                        throw new RaftException();
                    }
                    // 检查是否在集群中
                    if (!progresses.containsKey(id)) {
                        // 这里检查本节点是否还在集群以内，如果已经不在集群中了，不处理该消息直接返回。
                        // 这种情况出现在本节点已经通过配置变化被移除出了集群的场景。
                        return;
                    }
/*                    loop:
                    while (true) {
                        for (int i = 0; i < m.getEntries().size(); i++) {
                            Entry entry = m.getEntries().get(i);
                            if (entry.getType() == Entry.EntryType.ConfChange) {

                                if (pendingConf) {
                                    // 如果当前为还没有处理的配置变化请求，则其他配置变化的数据暂时忽略
                                    LOGGER.info("propose conf {} ignored since pending unapplied " +
                                            "configuration", entry.toString());
                                    // 将这个数据置空
                                    try {
                                        m.getEntries().set(i, new Entry());
                                    } catch (UnsupportedOperationException ex) {
                                        //传入的是不可变list的话
                                        m.setEntries(new ArrayList<>(m.getEntries()));
                                        continue loop;
                                    }
                                }
                                // 置位有还没有处理的配置变化数据
                                pendingConf = true;
                            }
                        }
                        break;
                    }*/
                    // 添加数据到log中
                    appendEntries(m.getEntries());
                    // 向集群其他节点广播append消息
                    broadcastAppend();
                    return;
                case MsgReadIndex:
                    if (quorum() > 1) {
                        // 这个表达式用于判断是否commttied log索引对应的term是否与当前term不相等，
                        // 如果不相等说明这是一个新的leader，而在它当选之后还没有提交任何数据
                        if (raftLog.zeroTermOnErrCompacted(raftLog.getCommitted()) != term) {
                            return;
                        }
                        readOnly.addRequest(raftLog.getCommitted(), m);
                        broadcastHeartbeatWithCtx(m.getEntries().get(0).getData());
                    } else {
                        readStates.add(new ReadState(raftLog.getCommitted(),
                                m.getEntries().get(0).getData()));
                    }
                    return;
            }
            // 检查消息发送者当前是否在集群中
            if (!progresses.containsKey(m.getFrom())) {
                LOGGER.info("{} no progress available for {}", id, m.getFrom());
                return;
            }

            Progress progress = progresses.get(m.getFrom());
            if (progress == null) {
                LOGGER.info("{} no progress available for {}", id, m.getFrom());
                return;
            }

            switch (m.getType()) {
                case MsgAppResp:    // 对append消息的应答
                    // 置位该节点当前是活跃的
                    progress.setRecentActive(true);
                    if (m.isReject()) {
                        // 如果拒绝了append消息，说明term、index不匹配
                        LOGGER.info("{} received msgApp rejection(lastindex: {}) from {} for " +
                                "index {}", id, m.getRejectHint(), m.getFrom(), m.getIndex());
                        // rejecthint带来的是拒绝该app请求的节点，其最大日志的索引
                        if (progress.maybeDecrTo(m.getIndex(), m.getRejectHint())) {
                            // 尝试回退关于该节点的Match、Next索引
                            LOGGER.info("{} decreased progress of {} to [{}]", id, m.getFrom(),
                                    progress);
                            if (progress.getState() == Progress.ProgressState.Replicate) {
                                progress.becomeProbe();
                            }
                            // 再次发送append消息
                            sendAppend(m.getFrom());
                        }
                    } else {// 通过该append请求
                        boolean oldPaused = progress.isPaused();
                        // 如果该节点的索引发生了更新
                        if (progress.maybeUpdate(m.getIndex())) {
                            switch (progress.getState()) {
                                case Probe:
                                    // 如果当前该节点在探测状态，切换到可以接收副本状态
                                    progress.becomeReplicate();
                                    break;
                                case Snapshot:
                                    // 如果当前该接在在接受快照状态，而且已经快照数据同步完成了
                                    if (progress.needSnapshotAbort()) {
                                        progress.becomeProbe();
                                        LOGGER.debug("{} snapshot aborted, resumed sending " +
                                                        "replication messages to {} [{}]"
                                                , id, m.getFrom(), progress);
                                    }
                                    break;
                                case Replicate:
                            }
                            if (maybeCommit()) {
                                // 如果可以commit日志，那么广播append消息
                                broadcastAppend();
                            } else if (oldPaused) {
                                // 如果该节点之前状态是暂停，继续发送append消息给它
                                sendAppend(m.getFrom());
                            }
                        }
                    }
                    break;
                case MsgHeartbeatResp:
                    // 该节点当前处于活跃状态
                    progress.setRecentActive(true);
                    // 这里调用resume是因为当前可能处于probe状态，
                    // 而这个状态在两个heartbeat消息的间隔期只能收一条同步日志消息，因此在收到HB消息时就停止pause标记
                    progress.resume();

                    /// 该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
                    if (progress.getMatch() < raftLog.lastIndex()) {
                        sendAppend(m.getFrom());
                    }

                    if (readOnly.recvAck(m) < quorum())
                        // 小于集群半数以上就返回不往下走了
                        return;
                    // 调用advance函数尝试丢弃已经被确认的read index状态
                    List<ReadIndexStatus> rss = readOnly.advance(m);
                    for (ReadIndexStatus rs : rss) {
                        Message req = rs.getRequest();
                        if (req.getFrom() == NONE || req.getFrom() == id) {
                            // 如果来自本地
                            readStates.add(new ReadState(rs.getIndex(),
                                    req.getEntries().get(0).getData()));
                        } else
                            // 否则就是来自外部，需要应答
                            send(Message.builder()
                                    .to(req.getFrom())
                                    .type(Message.MessageType.MsgReadIndexResp)
                                    .index(rs.getIndex())
                                    .entries(req.getEntries())
                                    .build());
                    }
                    break;
                case MsgSnapStatus:
                    if (progress.getState() != Progress.ProgressState.Snapshot)
                        return;
                    if (!m.isReject()) {
                        progress.becomeProbe();
                        LOGGER.debug("{} snapshot succeeded, resumed sending replication messages" +
                                " to {} [{}]", id, m.getFrom(), progress);
                    } else {
                        progress.snapshotFailure();
                        progress.becomeProbe();
                        LOGGER.debug("{} snapshot failed, resumed sending replication messages" +
                                " to {} [{}]", id, m.getFrom(), progress);
                    }
                    // 先暂停等待下一次被唤醒
                    progress.pause();
                    break;
                case MsgUnreachable:
                    if (progress.getState() == Progress.ProgressState.Replicate)
                        progress.becomeProbe();
                    LOGGER.debug("{} failed to send message to {} because it is unreachable [{}]"
                            , id, m.getFrom(), progress);
            }
        };
    }

    private void broadcastAppend() {
        progresses.keySet().forEach(id -> {
            if (id != this.id)
                sendAppend(id);
        });
    }

    private void sendAppend(Long to) {
        Progress progress = progresses.get(to);
        if (progress.isPaused()) {
            LOGGER.info("node {} paused", to);
            return;
        }
        Message.MessageBuilder messageBuilder = Message.builder();
        messageBuilder.to(to);
        boolean compacted = false;
        // 从该节点的Next的上一条数据获取term
        long term;
        try {
            term = raftLog.term(progress.getNext() - 1);
        } catch (CompactedException ex) {
            compacted = true;
            term = 0;
        }
        List<Entry> entries = null;
        try {
            // 获取从该节点的Next之后的entries，总和不超过maxMsgSize
            entries = raftLog.entries(((int) progress.getNext()), maxMsgSize);
        } catch (CompactedException e) {
            compacted = true;
        }

        //之前的数据都写到快照里了，尝试发送快照数据过去
        if (compacted) {
            if (!progress.isRecentActive()) {
                // 如果该节点当前不可用
                LOGGER.debug("ignore sending snapshot to {} since it is not recently active", to);
                return;
            }
            // 尝试发送快照
            messageBuilder.type(Message.MessageType.MsgSnap);
            Snapshot snapshot;
            try {
                snapshot = raftLog.snapshot();
            } catch (Throwable e) {
                if (e instanceof SnapshotTemporarilyUnavailableException) {
                    LOGGER.debug("{} failed to send snapshot to {} because snapshot is " +
                            "temporarily unavailable", this.id, to);
                    return;
                } else
                    throw new RaftException(e);
            }
            // 不能发送空快照
            if (isEmptySnapshot(snapshot))
                throw new RaftException("need non-empty snapshot");
            messageBuilder.snapshot(snapshot);
            long snapIdx = snapshot.getMetadata().getIndex();
            long snapTerm = snapshot.getMetadata().getTerm();
            LOGGER.debug("{} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to " +
                            "{} [{}]", id, raftLog.firstIndex(), raftLog.getCommitted(), snapIdx,
                    snapTerm, to
                    , progress);
            // 该节点进入接收快照的状态
            progress.becomeSnapshot(snapIdx);
            LOGGER.debug("{} paused sending replication messages to {} [{}]", id, to, progress);
        } else {
            // 否则就是简单的发送append消息
            messageBuilder
                    .type(Message.MessageType.MsgApp)
                    .index(progress.getNext() - 1)
                    .logTerm(term)
                    .entries(entries)
                    // append消息需要告知当前leader的commit索引
                    .commit(raftLog.getCommitted());
            if (Utils.notEmpty(entries)) {
                // 如果发送过去的entries不为空
                switch (progress.getState()) {
                    case Replicate:
                        // 如果该节点在接受副本的状态
                        // 得到待发送数据的最后一条索引
                        long last = entries.get(entries.size() - 1).getIndex();
                        // 直接使用该索引更新Next索引
                        progress.optimisticUpdate(last);
                        break;
                    case Probe:
                        // 在probe状态时，每次只能发送一条app消息
                        progress.pause();
                        break;
                    default:
                        LOGGER.error("{} is sending append in unhandled state {}", id,
                                progress.getState());
                        throw new RaftException();
                }
            }
        }
        send(messageBuilder.build());
    }

    private boolean isEmptySnapshot(Snapshot snapshot) {
        return snapshot.getMetadata().getIndex() == 0;
    }

    /**
     * @return 检查集群的可用数，当小于半数的节点不可用时返回false
     */
    private boolean checkQuorumActive() {
        int ack = 0;
        for (Map.Entry<Long, Progress> entry : progresses.entrySet()) {
            long id = entry.getKey();
            Progress progress = entry.getValue();
            if (id == this.id) {
                ++ack;
                continue;
            }
            if (progress.isRecentActive())
                ++ack;
            else if (raftLog.getStorage() instanceof MemoryStorage) { //对于内存存储，节点下限之后要把match归0
                if(progress.getMatch() != 0) {
                    progress.setMatch(0);
                    LOGGER.info("progress setMatch 0 for memoryStorage");
                }
            }
            progress.setRecentActive(false);
        }
        return ack >= quorum();
    }

    //bcastHeartbeat sends RPC, without entries to all the peers.
    private void broadcastHeartbeat() {
        String lastCtx = readOnly.lastPendingRequestCtx();
        if (lastCtx.length() == 0)
            broadcastHeartbeatWithCtx(null);
        else
            broadcastHeartbeatWithCtx(lastCtx.getBytes());
    }

    private void broadcastHeartbeatWithCtx(byte[] ctx) {
        progresses.keySet().forEach(id -> {
            if (id != this.id)
                sendHeartBeat(id, ctx);
        });
    }

    private void sendHeartBeat(Long to, byte[] ctx) {
        // commit index取需要发送过去的节点的match已经当前leader的commited中的较小值
        long commit = Math.min(progresses.get(to).getMatch(), raftLog.getCommitted());
        send(Message.builder()
                .to(to)
                .type(Message.MessageType.MsgHeartbeat)
                .commit(commit)
                .context(ctx)
                .build());
    }

    private StepFunction stepFollower() {
        return (raft, m) -> {
            switch (m.getType()) {
                case MsgProp:
                    if (lead == NONE) {
                        // 没有leader则提交失败，忽略
                        LOGGER.info("{} no leader at term {}; dropping proposal", id, term);
                        return;
                    }
                    // 向leader转发
                    m.setTo(lead);
                    send(m);
                    break;
                case MsgApp:
                    // append消息
                    // 收到leader的app消息，重置选举tick计时器，因为这样证明leader还存活
                    electionElapsed = 0;
                    lead = m.getFrom();
                    handleAppendEntries(m);
                    break;
                case MsgHeartbeat:
                    electionElapsed = 0;
                    lead = m.getFrom();
                    handleHeartbeat(m);
                    break;
                case MsgSnap:
                    electionElapsed = 0;
                    lead = m.getFrom();
                    handleSnapshot(m);
                    break;
                case MsgReadIndex:
                    if (lead == NONE) {
                        LOGGER.info("{} no leader at term {}; dropping index reading msg", id,
                                term);
                        return;
                    }
                    m.setTo(lead);
                    send(m);
                    break;
                case MsgReadIndexResp:
                    if (Utils.isEmpty(m.getEntries()) || m.getEntries().size() != 1) {
                        LOGGER.error("{} invalid format of MsgReadIndexResp from {}, entries " +
                                "count: {}", id, m.getFrom(), m.getEntries().size());
                        return;
                    }
                    readStates.add(new ReadState(m.getIndex(), m.getEntries().get(0).getData()));
            }
        };
    }

    private void handleSnapshot(Message m) {
        long snapIdx = m.getSnapShot().getMetadata().getIndex();
        long snapTerm = m.getSnapShot().getMetadata().getTerm();
        // 注意这里成功与失败，只是返回的Index参数不同
        if (restore(m.getSnapShot())) {
            LOGGER.info("{} [commit: {}] restored snapshot [index: {}, term: {}]", id,
                    raftLog.getCommitted(), snapIdx, snapTerm);
            send(Message.builder()
                    .to(m.getFrom())
                    .type(Message.MessageType.MsgAppResp)
                    .index(raftLog.lastIndex())
                    .build()
            );
        } else {
            LOGGER.info("{} [commit: {}] ignore snapshot [index: {}, term: {}]", id,
                    raftLog.getCommitted(), snapIdx, snapTerm);
            send(Message.builder()
                    .to(m.getFrom())
                    .type(Message.MessageType.MsgAppResp)
                    .index(raftLog.getCommitted())
                    .build()
            );
        }
    }

    public boolean restore(Snapshot s) {
        if (s.getMetadata().getIndex() <= raftLog.getCommitted())
            return false;
        // matchTerm返回true，说明本节点的日志中已经有对应的日志了
        if (raftLog.term(s.getMetadata().getIndex()) == s.getMetadata().getTerm()) {
            LOGGER.info("{} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to " +
                            "snapshot [index: {}, term: {}]", id, raftLog.getCommitted(),
                    raftLog.lastIndex(), raftLog.lastTerm(), s.getMetadata().getIndex(),
                    s.getMetadata().getTerm());
            // 提交到快照所在的索引
            raftLog.commitTo(s.getMetadata().getIndex());
            // 为什么这里返回false？
            return false;
        }
        LOGGER.info("{} [commit: {}, lastindex: {}, lastterm: {}] " +
                        "starts to restore snapshot [index: {}, term: {}]", id,
                raftLog.getCommitted(),
                raftLog.lastIndex(), raftLog.lastTerm(), s.getMetadata().getIndex(),
                s.getMetadata().getTerm());
        raftLog.restore(s);
        // 包括集群中其他节点的状态也使用快照中的状态数据进行恢复
        for (Long node : s.getMetadata().getConfState().getNodes()) {
            long match = 0;
            long next = raftLog.lastIndex() + 1;
            if (node.equals(id))
                match = next - 1;
            progresses.put(node, new Progress(next, match));
            LOGGER.info("{} restored progress of {} [{}]", id, node, progresses.get(id));
        }
        return true;
    }

    /**
     * 处理HB消息
     *
     * @param m m
     */
    private void handleHeartbeat(Message m) {
        raftLog.commitTo(m.getCommit());
        send(Message.builder()
                .to(m.getFrom())
                .type(Message.MessageType.MsgHeartbeatResp)
                // 要把HB消息带过来的context原样返回
                .context(m.getContext())
                .build());
    }

    public void deleteProgress(long id) {
        progresses.remove(id);
    }

    private void handleAppendEntries(Message m) {
        // 先检查消息消息的合法性
        if (m.getIndex() < raftLog.getCommitted()) {
            send(Message.builder()
                    .to(m.getFrom())
                    .type(Message.MessageType.MsgAppResp)
                    .index(raftLog.getCommitted())
                    .build());
            return;
        }
        LOGGER.info("{} to {} index {}", m.getFrom(), id, m.getIndex());
        // 尝试添加到日志模块中
        long mLastIdx = raftLog.maybeAppend(m.getIndex(), m.getLogTerm(), m.getCommit(),
                m.getEntries());
        if (mLastIdx != 0)
            // 添加成功，返回的index是添加成功之后的最大index
            send(Message.builder()
                    .to(m.getFrom())
                    .type(Message.MessageType.MsgAppResp)
                    .index(mLastIdx)
                    .build());
        else {
            LOGGER.debug("{} [logterm: {}, index: {}] rejected msgApp [logterm: {}, index: {}] " +
                            "from {}", id, raftLog.zeroTermOnErrCompacted(m.getIndex()),
                    m.getIndex(), m.getLogTerm(),
                    m.getIndex(), m.getFrom());
            send(Message.builder()
                    .to(m.getFrom())
                    .type(Message.MessageType.MsgAppResp)
                    .index(m.getIndex())
                    .reject(true)
                    .rejectHint(raftLog.lastIndex())
                    .build()
            );
        }
    }

    private StepFunction stepCandidate() {
        return (raft, m) -> {
            switch (m.getType()) {
                case MsgProp:
                    // 当前没有leader，所以忽略掉提交的消息
                    LOGGER.info("{} no leader at term {}; dropping proposal", id, term);
                    return;
                case MsgApp:
                    // 收到append消息，说明集群中已经有leader，转换为follower
                    becomeFollower(term, m.getFrom());
                    handleAppendEntries(m);
                    break;
                case MsgHeartbeat:
                    // 收到HB消息，说明集群已经有leader，转换为follower
                    becomeFollower(term, m.getFrom());
                    handleHeartbeat(m);
                    break;
                case MsgSnap:
                    // 收到快照消息，说明集群已经有leader，转换为follower
                    becomeFollower(term, m.getFrom());
                    handleSnapshot(m);
                    break;
                case MsgVoteResp:
                    // 计算当前集群中有多少节点给自己投了票
                    int quorum = quorum();
                    int grant = poll(m.getFrom(), m.getType(), !m.isReject());
                    LOGGER.info("{} [quorum:{}] has received {} {} votes and {} vote rejections",
                            id, quorum, grant, m.getType(), votes.size() - grant);
                    if (quorum == grant) {
                        becomeLeader();
                        broadcastAppend();
                    } else if (quorum == votes.size() - grant) {// 如果是半数以上节点拒绝了投票
                        // 变成follower
                        becomeFollower(term, NONE);
                    }
            }
        };
    }

    /**
     * follower以及candidate的tick函数，在r.electionTimeout之后被调用
     *
     * @return tickFunction
     */
    private TickFunction tickElection() {
        return () -> {
            ++electionElapsed;
            if (promotable() && pastElectionTimeout()) {
                // 如果可以被提升为leader，同时选举时间也到了
                electionElapsed = 0;
                // 发送HUP消息是为了重新开始选举
                step(
                        Message.builder().from(id).type(Message.MessageType.MsgHup).build());
            }
        };
    }

    /**
     * tickHeartbeat是leader的tick函数
     */
    private TickFunction tickHeartBeat() {
        return () -> {
            ++heartbeatElapsed;
            ++electionElapsed;
            if (electionElapsed >= electionTimeout) {
                // 如果超过了选举时间
                electionElapsed = 0;
                if (checkQuorum)
                    step(Message.builder().from(id).type(Message.MessageType.MsgCheckQuorum).build());
            }
            if (state != Node.NodeState.Leader)
                return;
            if (heartbeatElapsed >= heartbeatTimeout) {
                // 向集群中其他节点发送广播消息
                heartbeatElapsed = 0;
                // 尝试发送MsgBeat消息
                step(
                        Message.builder().from(id).type(Message.MessageType.MsgBeat).build());
            }
        };
    }

    public void addNodes(Long peer) {
        progresses.computeIfAbsent(peer, k -> new Progress(0, raftLog.lastIndex() + 1));
    }

    public SoftState softState() {
        return new SoftState(lead, state);
    }


    public HardState hardState() {
        return new HardState(raftLog.getCommitted(), term, vote);
    }


    public boolean hasLeader() {
        return lead != NONE;
    }

    public List<Long> nodes() {
        return progresses.keySet().stream().sorted().collect(Collectors.toList());
    }

    public void tick() {
        tickFunc.tick();
    }

    public long getId() {
        return id;
    }

    public long getTerm() {
        return term;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public Map<Long, Progress> getProgresses() {
        return progresses;
    }

    public int getElectionElapsed() {
        return electionElapsed;
    }

    public List<ReadState> getReadStates() {
        return readStates;
    }

    public ReadOnly getReadOnly() {
        return readOnly;
    }

    public int getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    public Node.NodeState getState() {
        return state;
    }

    public List<Message> getMessages() {
        return messages;
    }

    public long getLead() {
        return lead;
    }

    public int getElectionTimeout() {
        return electionTimeout;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public void setProgresses(Map<Long, Progress> progresses) {
        this.progresses = progresses;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }

    public void setCheckQuorum(boolean checkQuorum) {
        this.checkQuorum = checkQuorum;
    }

    public void setStepFunc(StepFunction stepFunc) {
        this.stepFunc = stepFunc;
    }

    public void setRandomizedElectionTimeout(int randomizedElectionTimeout) {
        this.randomizedElectionTimeout = randomizedElectionTimeout;
    }

    public void setReadStates(List<ReadState> readStates) {
        this.readStates = readStates;
    }
}
