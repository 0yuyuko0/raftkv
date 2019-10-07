package com.yuyuko.raftkv.raft.node;

import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.utils.Utils;
import com.yuyuko.utils.concurrent.Chan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.yuyuko.utils.concurrent.Select.select;

public class DefaultNode implements Node {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    // 提交本地请求数据用的channel
    private Chan<Message> propChan;

    // 接收外部请求数据用的channel
    private Chan<Message> recvChan;

    // 接收配置更新的channel
    private Chan<ConfChange> confChan;

    // 接收最新配置状态的channel
    private Chan<ConfState> confStateChan;

    private Chan<Ready> readyChan;

    private Chan<Object> advanceChan;

    private Chan<Object> tickChan;

    private Chan<Object> doneChan;

    private Chan<Object> stopChan;

    private Chan<Chan<Status>> statusChan;


    public static Node startNode(Config config, List<Peer> peers) {
        Raft raft = Raft.newRaft(config);
        // 初次启动以term为1来启动
        raft.becomeFollower(1, Raft.NONE);
        for (Peer peer : peers) {
            ConfChange cc = new ConfChange(ConfChange.ConfChangeType.AddNode,
                    peer.getId(), peer.getContext());
            byte[] marshal = cc.marshal();
            raft.getRaftLog().append(List.of(
                    new Entry(raft.getRaftLog().lastIndex() + 1, 1, Entry.EntryType.ConfChange,
                            marshal)
            ));
        }

        //使初始化entries提交
        raft.getRaftLog().setCommitted(raft.getRaftLog().lastIndex());

        for (Peer peer : peers) {
            raft.addNodes(peer.getId());
        }
        DefaultNode node = newNode();
        Thread thread = new Thread(() -> node.run(raft));
        thread.setName("NodeEventLoop");
        thread.start();
        return node;
    }

    static DefaultNode newNode() {
        DefaultNode node = new DefaultNode();
        node.propChan = new Chan<>();
        node.readyChan = new Chan<>();
        node.confChan = new Chan<>();
        node.readyChan = new Chan<>();
        node.recvChan = new Chan<>();
        node.confStateChan = new Chan<>();
        node.doneChan = new Chan<>();
        node.stopChan = new Chan<>();
        node.advanceChan = new Chan<>();
        node.statusChan = new Chan<>();
        node.tickChan = new Chan<>(128);
        return node;
    }

    void run(Raft raft) {
        Chan<Message> propChan = null;
        Chan<Ready> readyChan;
        AtomicReference<Chan<Object>> advanceChan = new AtomicReference<>();
        final AtomicLong prevLastUnstableIdx = new AtomicLong();
        final AtomicLong prevLastUnstableTerm = new AtomicLong();
        final AtomicBoolean hasPrevLastUnstableIdx = new AtomicBoolean();
        final AtomicLong prevSnapIdx = new AtomicLong();
        AtomicReference<Ready> rd = new AtomicReference<>();

        long lead = Raft.NONE;
        final AtomicReference<SoftState> prevSoftSt = new AtomicReference<>(raft.softState());
        final AtomicReference<HardState> prevHardSt = new AtomicReference<>(HardState.EMPTY);

        AtomicBoolean stop = new AtomicBoolean();
        while (true) {
            if (advanceChan.get() != null)
                // advance channel不为空，说明还在等应用调用Advance接口通知已经处理完毕了本次的ready数据
                readyChan = null;
            else {
                rd.set(new Ready(raft, prevSoftSt.get(), prevHardSt.get()));
                if (rd.get().containsUpdate())
                    readyChan = this.readyChan;
                else
                    readyChan = null;
            }
            if (lead != raft.getLead()) {
                // 如果leader发生了变化
                if (raft.hasLeader()) {// 如果原来有leader
                    if (lead == Raft.NONE)
                        LOGGER.info("raft.node: {} elected leader {} at term {}", raft.getId(),
                                raft.getLead(), raft.getTerm());
                    else {
                        LOGGER.info("raft.node: {} changed leader from {} to {} at term {}",
                                raft.getId(), lead, raft.getLead(), raft.getTerm());
                    }
                    // 有leader，那么可以进行数据提交，prop channel不为空
                    propChan = this.propChan;
                } else {
                    // 否则，prop channel为空
                    LOGGER.info("raft.node: {} lost leader {} at term {}", raft.getId(), lead,
                            raft.getTerm());
                    propChan = null;
                }
                lead = raft.getLead();
            }

            select()
                    .forChan(propChan) // 处理本地收到的提交值
                    .read(m -> {
                        m.setFrom(raft.getId());
                        raft.step(m);
                    })

                    .forChan(recvChan)
                    .read(m -> {
                        // 处理其他节点发送过来的提交值
                        if (raft.getProgresses().containsKey(m.getFrom())
                                || !Message.isResponseMsg(m.getType())) {
                            // 需要确保节点在集群中 或者 不是应答类消息的情况下才进行处理
                            raft.step(m);
                        }
                    })

                    .forChan(confChan)
                    // 接收到配置发生变化的消息

                    .read(cc -> {
                        if (cc.getNodeId() == Raft.NONE) {
                            // NodeId为空的情况，只需要直接返回当前的nodes就好
                            raft.resetPendingConf();
                            select()
                                    .forChan(confStateChan).write(new ConfState(raft.nodes()))
                                    .forChan(doneChan).read()
                                    .start();
                            return;
                        }
                        switch (cc.getType()) {
                            case AddNode:
                                raft.addNodes(cc.getNodeId());
                        }
                        // 返回当前nodes
                        select()
                                .forChan(confStateChan).write(new ConfState(raft.nodes()))
                                .forChan(doneChan).read()
                                .start();
                    })

                    .forChan(tickChan)
                    .read(tick -> raft.tick())

                    .forChan(readyChan)
                    .write(rd.get(), () -> {
                        Ready rdL = rd.get();

                        // 以下先把ready的值保存下来，等待下一次循环使用，或者当advance调用完毕之后用于修改raftLog的

                        if (rdL.getSoftState() != null)
                            prevSoftSt.set(rdL.getSoftState());

                        if (Utils.notEmpty(rdL.getEntries())) {
                            // 保存上一次还未持久化的entries的index、term

                            prevLastUnstableIdx.set(
                                    rdL.getEntries().get(rdL.getEntries().size() - 1).getIndex());
                            prevLastUnstableTerm.set(
                                    rdL.getEntries().get(rdL.getEntries().size() - 1).getTerm());
                            hasPrevLastUnstableIdx.set(true);
                        }
                        if (!Utils.isEmptyHardState(rdL.getHardState()))
                            prevHardSt.set(rdL.getHardState());
                        if (!Utils.isEmptySnapshot(rdL.getSnapshot())) {
                            prevSnapIdx.set(rdL.getSnapshot().getMetadata().getIndex());
                        }
                        raft.setMessages(new ArrayList<>());
                        raft.setReadStates(new ArrayList<>());

                        advanceChan.set(this.advanceChan);
                    })

                    .forChan(advanceChan.get())
                    .read(data -> {
                        if (prevHardSt.get().getCommit() != 0)
                            raft.getRaftLog().appliedTo(prevHardSt.get().getCommit());
                        if (hasPrevLastUnstableIdx.get()) {
                            raft.getRaftLog().stableTo(prevLastUnstableIdx.get(),
                                    prevLastUnstableTerm.get());
                            hasPrevLastUnstableIdx.set(false);
                        }
                        raft.getRaftLog().stableSnapTo(prevSnapIdx.get());
                        advanceChan.set(null);
                    })
                    .forChan(statusChan)
                    .read(chan -> chan.send(getStatus(raft)))

                    .forChan(stopChan)
                    .read(o -> {
                        doneChan.close();
                        stop.set(true);
                    }).start();
            if (stop.get())
                return;
        }
    }

    Status getStatus(Raft raft) {
        Status status = new Status();
        status.setId(raft.getId());
        status.setSoftState(raft.softState());
        status.setHardState(raft.hardState());
        status.setApplied(raft.getRaftLog().getApplied());
        if (raft.getState() == NodeState.Leader) {
            status.setProgresses(new HashMap<>(raft.getProgresses()));
        }
        return status;
    }

    @Override
    public Status status() {
        Chan<Status> chan = new Chan<>();
        AtomicReference<Status> res = new AtomicReference<>();
        select()
                .forChan(statusChan).write(chan, () -> res.set(chan.receive()))
                .forChan(doneChan).read(data -> res.set(new Status()))
                .start();
        return res.get();
    }

    @Override
    public void tick() {
        Logger logger = LOGGER;
        select()
                .forChan(tickChan).write(new Object())
                .forChan(doneChan).read()
                .onDefault(
                        () -> LOGGER.debug("A tick missed to fire. Node blocks too long!")
                )
                .start();
    }

    @Override
    public void campaign() {
        stepInternal(
                Message.builder()
                        .type(Message.MessageType.MsgHup)
                        .build()
        );
    }

    @Override
    public void propose(byte[] data) {
        stepInternal(
                Message.builder()
                        .type(Message.MessageType.MsgProp)
                        .entries(List.of(new Entry(data)))
                        .build()
        );
    }

    public void stepInternal(Message m) {
        Chan<Message> c = recvChan;
        if (m.getType() == Message.MessageType.MsgProp)
            c = propChan;

        AtomicBoolean done = new AtomicBoolean();
        select()
                .forChan(c).write(m)
                .forChan(doneChan).read(o -> done.set(true))
                .start();
        if (done.get())
            throw new NodeStopException();
    }

    @Override
    public void step(Message msg) {
        if (Message.isLocalMsg(msg.getType()))
            return;
        stepInternal(msg);
    }

    @Override
    public Chan<Ready> ready() {
        return readyChan;
    }

    @Override
    public void advance() {
        select()
                .forChan(advanceChan).write(null)
                .forChan(doneChan).read()
                .start();
    }

    @Override
    public void readIndex(byte[] readCtx) {
        stepInternal(
                Message.builder()
                        .type(Message.MessageType.MsgReadIndex)
                        .entries(List.of(new Entry(readCtx)))
                        .build()
        );
    }

    @Override
    public ConfState applyConfChange(ConfChange confChange) {
        AtomicReference<ConfState> confState = new AtomicReference<>();
        select()
                .forChan(confChan).write(confChange)
                .forChan(doneChan).read()
                .start();
        select()
                .forChan(confStateChan).read(confState::set)
                .forChan(doneChan).read()
                .start();
        return confState.get();
    }

    @Override
    public void proposeConfChange(ConfChange cc) {
        step(
                Message.builder()
                        .type(Message.MessageType.MsgProp)
                        .entries(List.of(new Entry(Entry.EntryType.ConfChange, cc.marshal())))
                        .build()
        );
    }

    @Override
    public void stop() {
        LOGGER.info("node stop");
        AtomicBoolean done = new AtomicBoolean();
        select()
                .forChan(stopChan).write(null)
                .forChan(doneChan).read(o -> done.set(true))
                .start();
        if (done.get())
            return;
        doneChan.receive();
    }

    void setPropChan(Chan<Message> propChan) {
        this.propChan = propChan;
    }

    void setRecvChan(Chan<Message> recvChan) {
        this.recvChan = recvChan;
    }

    Chan<Message> getPropChan() {
        return propChan;
    }

    Chan<Message> getRecvChan() {
        return recvChan;
    }
}
