package com.yuyuko.raftkv.server.raft;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Config;
import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.node.*;
import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.raft.storage.MemoryStorage;
import com.yuyuko.raftkv.raft.storage.Snapshot;
import com.yuyuko.raftkv.raft.utils.Tuple;
import com.yuyuko.raftkv.raft.utils.Utils;
import com.yuyuko.raftkv.remoting.server.NettyServer;
import com.yuyuko.utils.concurrent.Chan;
import com.yuyuko.utils.concurrent.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private Chan<byte[]> proposeChan;

    private Chan<byte[]> readIndexChan;

    private Chan<ReadState> readStateChan;

    private Chan<ConfChange> confChangeChan;

    private Chan<byte[]> commitChan;

    private long id;

    private List<String> peers;

    private long lastIndex;

    private ConfState confState;

    private long snapshotIndex;

    private long appliedIndex;

    private Node node;

    private MemoryStorage storage = MemoryStorage.newMemoryStorage();

    private Timer timer = new Timer("tick-task", true);

    public static volatile RaftNode globalInstance;

    public static Tuple<Chan<byte[]>, Chan<ReadState>> newRaftNode(long id, List<String> peers,
                                                                   Chan<byte[]> proposeChan,
                                                                   Chan<byte[]> readIndexChan,
                                                                   Chan<ConfChange> confChangeChan) {
        RaftNode raftNode = new RaftNode();
        raftNode.proposeChan = proposeChan;
        raftNode.readIndexChan = readIndexChan;
        raftNode.confChangeChan = confChangeChan;
        raftNode.commitChan = new Chan<>();
        raftNode.readStateChan = new Chan<>();
        raftNode.id = id;
        raftNode.peers = peers;

        raftNode.startRaft();
        globalInstance = raftNode;

        return new Tuple<>(raftNode.commitChan, raftNode.readStateChan);
    }

    private void startRaft() {
        List<Peer> rPeers = new ArrayList<>();
        for (int i = 0; i < peers.size(); i++) {
            rPeers.add(new Peer(((long) (i + 1)), null));
        }
        Config config = new Config();
        config.setId(this.id);
        config.setCheckQuorum(true);
        config.setElectionTick(300);
        config.setHeartbeatTick(150);
        config.setStorage(storage);
        config.setMaxSizePerMsg(1024 * 1024);

        node = DefaultNode.startNode(config, rPeers);

        Thread thread = new Thread(this::serveChannels);
        thread.setName("ServerChannelEventLoop");

        thread.start();
    }

    private void serveChannels() {
        Snapshot snapshot = storage.snapshot();
        confState = snapshot.getMetadata().getConfState();
        snapshotIndex = snapshot.getMetadata().getIndex();
        appliedIndex = snapshot.getMetadata().getIndex();

        final Chan<Object> tickChan = new Chan<>();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                tickChan.send(null);
            }
        }, 0, 1);

        final ReentrantLock tickLock;
        try {
            Chan<Ready> ready = node.ready();
            Field lockF = null;
            lockF = ready.getClass().getDeclaredField("lock");
            lockF.setAccessible(true);
            tickLock = (ReentrantLock) lockF.get(ready);
            //这是怎么一回事？？？
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException();
        }

        Thread thread = new Thread(() -> {
            while (proposeChan != null && confChangeChan != null && readIndexChan != null) {
                Select.select()
                        .forChan(proposeChan)
                        .read(bytes -> {
                            if (bytes == null)
                                proposeChan = null;
                            else
                                node.propose(bytes);
                        })
                        .forChan(readIndexChan)
                        .read(bytes -> {
                            if (bytes == null)
                                readIndexChan = null;
                            else
                                node.readIndex(bytes);
                        })
                        .forChan(confChangeChan)
                        .read(cc -> {
                            if (cc == null)
                                confChangeChan = null;
                            else
                                node.proposeConfChange(cc);
                        })
                        .start();
            }
        });
        thread.setName("RaftNodeProposeThread");
        thread.start();

        while (true) {
            Select
                    .select()
                    .forChan(tickChan).read(data -> {
                //bug，只能这样修复了
                if (tickLock.isHeldByCurrentThread()) {
                    tickLock.unlock();
                }
                node.tick();
            }).forChan(node.ready()).read(rd -> {
                storage.append(rd.getEntries());
                if (Utils.notEmpty(rd.getMessages())) {
                    while (NettyServer.getGlobalInstance() == null) Thread.onSpinWait();
                    NettyServer.getGlobalInstance().sendMessageToPeer(rd.getMessages());
                }
                publishEntries(entriesToApply(rd.getCommittedEntries()));
                publishReadStates(rd.getReadStates());
                node.advance();
            }).start();
        }
    }

    private void publishReadStates(List<ReadState> readStates) {
        if (readStates == null)
            return;
        for (ReadState readState : readStates) {
            readStateChan.send(readState);
        }
    }

    private List<Entry> entriesToApply(List<Entry> entries) {
        if (entries == null || entries.size() == 0)
            return null;
        long firstIdx = entries.get(0).getIndex();
        if (firstIdx > appliedIndex + 1) {
            log.error("first index of committed entry[{}] should <= progress.appliedIndex[{}] 1",
                    firstIdx, appliedIndex);
            throw new RaftException();
        }
        if (appliedIndex - firstIdx + 1 < entries.size())
            return entries.subList((int) (appliedIndex - firstIdx + 1), entries.size());
        return null;
    }

    private void publishEntries(List<Entry> entries) {
        if (entries == null) return;
        for (Entry entry : entries) {
            switch (entry.getType()) {
                case ConfChange:
                    ConfChange confChange = ConfChange.unmarshal(entry.getData());
                    node.applyConfChange(confChange);
                    break;
                case Normal:
                    if (entry.getData() == null)
                        break;
                    commitChan.send(entry.getData());
            }
            appliedIndex = entry.getIndex();
        }
    }

    public Node getNode() {
        return node;
    }

    public static RaftNode getGlobalInstance() {
        return globalInstance;
    }
}