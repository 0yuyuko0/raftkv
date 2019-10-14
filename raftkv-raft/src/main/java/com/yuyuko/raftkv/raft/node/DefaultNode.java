package com.yuyuko.raftkv.raft.node;

import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.utils.Utils;
import com.yuyuko.selector.Channel;
import com.yuyuko.selector.SelectionKey;
import com.yuyuko.selector.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.yuyuko.selector.SelectionKey.*;


public class DefaultNode implements Node {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNode.class);

    // 提交本地请求数据用的channel
    private Channel<Message> propChan;

    // 接收外部请求数据用的channel
    private Channel<Message> recvChan;

    private Channel<Ready> readyChan;

    private Channel<Object> advanceChan;

    private Channel<Object> tickChan;

    private Channel<Object> doneChan;

    private Channel<Object> stopChan;

    private Channel<Channel<Status>> statusChan;


    public static Node startNode(Config config, List<Peer> peers) {
        Raft raft = Raft.newRaft(config);
        // 初次启动以term为1来启动
        raft.becomeFollower(1, Raft.NONE);

/*        for (Peer peer : peers) {
            ConfChange cc = new ConfChange(ConfChange.ConfChangeType.AddNode,
                    peer.getId(), peer.getContext());
            byte[] marshal = cc.marshal();
            raft.getRaftLog().append(List.of(
                    new Entry(raft.getRaftLog().lastIndex() + 1, 1, Entry.EntryType.ConfChange,
                            marshal)
            ));
        }

        //使初始化entries提交
        raft.getRaftLog().setCommitted(raft.getRaftLog().lastIndex());*/

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
        node.propChan = new Channel<>();
        node.readyChan = new Channel<>();
        node.recvChan = new Channel<>();
        node.doneChan = new Channel<>();
        node.stopChan = new Channel<>();
        node.advanceChan = new Channel<>();
        node.statusChan = new Channel<>();
        node.tickChan = new Channel<>(128);
        return node;
    }

    public void run(Raft raft) {
        Channel<Message> propChan = null;
        Channel<Ready> readyChan;
        Channel<Object> advanceChan = null;
        long prevLastUnstableIdx = 0;
        long prevLastUnstableTerm = 0;
        boolean hasPrevLastUnstableIdx = false;
        long prevSnapIdx = 0;
        Ready rd = null;

        long lead = Raft.NONE;
        SoftState prevSoftSt = (raft.softState());
        HardState prevHardSt = (HardState.EMPTY);

        while (true) {
            if (advanceChan != null)
                // advance channel不为空，说明还在等应用调用Advance接口通知已经处理完毕了本次的ready数据
                readyChan = null;
            else {
                rd = (new Ready(raft, prevSoftSt, prevHardSt));
                if (rd.containsUpdate())
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

            SelectionKey<?> key =
                    Selector.open()
                            .register(propChan, read())
                            .register(recvChan, read())
                            .register(tickChan, read())
                            .register(readyChan, write(rd))
                            .register(advanceChan, read())
                            .register(statusChan, read())
                            .register(stopChan, read())
                            .select();
            if (key.channel() == propChan) {
                Message m = key.data(Message.class);
                m.setFrom(raft.getId());
                raft.step(m);
            } else if (key.channel() == recvChan) {
                Message m = key.data(Message.class);
                if (raft.getProgresses().containsKey(m.getFrom())
                        || !Message.isResponseMsg(m.getType())) {
                    // 需要确保节点在集群中 或者 不是应答类消息的情况下才进行处理
                    raft.step(m);
                }
            } else if (key.channel() == tickChan) {
                raft.tick();
            } else if (key.channel() == readyChan) {
                Ready rdL = key.data(Ready.class);

                // 以下先把ready的值保存下来，等待下一次循环使用，或者当advance调用完毕之后用于修改raftLog的

                if (rdL.getSoftState() != null)
                    prevSoftSt = rdL.getSoftState();

                if (Utils.notEmpty(rdL.getEntries())) {
                    // 保存上一次还未持久化的entries的index、term

                    prevLastUnstableIdx =
                            rdL.getEntries().get(rdL.getEntries().size() - 1).getIndex();
                    prevLastUnstableTerm =
                            rdL.getEntries().get(rdL.getEntries().size() - 1).getTerm();
                    hasPrevLastUnstableIdx = true;
                }
                if (!Utils.isEmptyHardState(rdL.getHardState()))
                    prevHardSt = (rdL.getHardState());
                if (!Utils.isEmptySnapshot(rdL.getSnapshot())) {
                    prevSnapIdx = (rdL.getSnapshot().getMetadata().getIndex());
                }
                raft.setMessages(new ArrayList<>());
                raft.setReadStates(new ArrayList<>());

                advanceChan = (this.advanceChan);
            } else if (key.channel() == advanceChan) {
                if (prevHardSt.getCommit() != 0)
                    raft.getRaftLog().appliedTo(prevHardSt.getCommit());
                if (hasPrevLastUnstableIdx) {
                    raft.getRaftLog().stableTo(prevLastUnstableIdx,
                            prevLastUnstableTerm);
                    hasPrevLastUnstableIdx = false;
                }
                raft.getRaftLog().stableSnapTo(prevSnapIdx);
                advanceChan = null;
            } else if (key.channel() == statusChan) {
                Channel<Status> channel = ((Channel<Status>) key.data());
                channel.write(getStatus(raft));
            } else if (key.channel() == stopChan) {
                doneChan.close();
                return;
            } else
                throw new RuntimeException("unexpected selectkey channel");
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
        Channel<Status> chan = new Channel<>();
        SelectionKey<?> key =
                Selector.open()
                        .register(statusChan, write(chan))
                        .register(doneChan, read())
                        .select();
        if (key.channel() == statusChan)
            return chan.read();
        else
            return new Status();
    }

    @Override
    public void tick() {
        SelectionKey<?> key = Selector.open()
                .register(tickChan, write(new Object()))
                .register(doneChan, read())
                .fallback(fallback())
                .select();
        if (key.type() == FALLBACK)
            LOGGER.debug("A tick missed to fire. Node blocks too long!");
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
        Channel<Message> c = recvChan;
        if (m.getType() == Message.MessageType.MsgProp)
            c = propChan;

        SelectionKey<?> key = Selector.open()
                .register(c, write(m))
                .register(doneChan, read())
                .select();
        if (key.channel() == doneChan)
            throw new NodeStopException();
    }

    @Override
    public void step(Message msg) {
        if (Message.isLocalMsg(msg.getType()))
            return;
        stepInternal(msg);
    }

    @Override
    public Channel<Ready> ready() {
        return readyChan;
    }

    @Override
    public void advance() {
        Selector.open()
                .register(advanceChan, write(new Object()))
                .register(doneChan, read())
                .select();
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
    public void stop() {
        LOGGER.info("node stop");
        SelectionKey<?> key = Selector.open()
                .register(stopChan, write(null))
                .register(doneChan, read())
                .select();
        if (key.channel() == doneChan)
            return;
        doneChan.read();
    }

    void setPropChan(Channel<Message> propChan) {
        this.propChan = propChan;
    }

    void setRecvChan(Channel<Message> recvChan) {
        this.recvChan = recvChan;
    }

    Channel<Message> getPropChan() {
        return propChan;
    }

    Channel<Message> getRecvChan() {
        return recvChan;
    }
}
