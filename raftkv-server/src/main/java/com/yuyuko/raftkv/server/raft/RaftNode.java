package com.yuyuko.raftkv.server.raft;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.core.ConfState;
import com.yuyuko.raftkv.raft.core.Config;
import com.yuyuko.raftkv.raft.core.Entry;
import com.yuyuko.raftkv.raft.core.Message;
import com.yuyuko.raftkv.raft.node.*;
import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.raft.storage.MemoryStorage;
import com.yuyuko.raftkv.raft.storage.Snapshot;
import com.yuyuko.raftkv.raft.utils.Tuple;
import com.yuyuko.raftkv.raft.utils.Utils;
import com.yuyuko.raftkv.remoting.peer.PeerMessageProcessor;
import com.yuyuko.raftkv.remoting.peer.PeerMessageSender;
import com.yuyuko.raftkv.remoting.server.NettyServer;
import com.yuyuko.raftkv.server.server.Server;
import com.yuyuko.raftkv.server.utils.Triple;
import com.yuyuko.selector.Channel;
import com.yuyuko.selector.SelectionKey;
import com.yuyuko.selector.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.yuyuko.selector.SelectionKey.read;

public class RaftNode implements PeerMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private Channel<byte[]> proposeChan;

    private Channel<byte[]> readIndexChan;

    private Channel<ReadState> readStateChan;

    private Channel<byte[]> commitChan;

    private long id;

    private List<Long> peers;

    private long lastIndex;

    private ConfState confState;

    private long snapshotIndex;

    private long appliedIndex;

    private Node node;

    private MemoryStorage storage = MemoryStorage.newMemoryStorage();

    private Timer timer = new Timer("TickTask", true);

    public static Triple<Channel<byte[]>, Channel<ReadState>, PeerMessageProcessor> newRaftNode(long id,
                                                                                                List<Long> peers,
                                                                                                Channel<byte[]> proposeChan,
                                                                                                Channel<byte[]> readIndexChan) {
        RaftNode raftNode = new RaftNode();
        raftNode.proposeChan = proposeChan;
        raftNode.readIndexChan = readIndexChan;
        raftNode.commitChan = new Channel<>();
        raftNode.readStateChan = new Channel<>();
        raftNode.id = id;
        raftNode.peers = peers;

        raftNode.startRaft();
        return new Triple<>(raftNode.commitChan, raftNode.readStateChan, raftNode);
    }

    private void startRaft() {
        List<Peer> rPeers =
                peers.stream().map(id -> new Peer(id, null)).collect(Collectors.toList());
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

        final Channel<Object> tickChan = new Channel<>();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                tickChan.write(null);
            }
        }, 0, 1);

        Thread thread = new Thread(() -> {
            while (proposeChan != null && readIndexChan != null) {
                SelectionKey<?> key = Selector.open()
                        .register(proposeChan, read())
                        .register(readIndexChan, read())
                        .select();
                if (key.channel() == proposeChan) {
                    byte[] bytes = key.data(byte[].class);
                    if (bytes == null)
                        proposeChan = null;
                    else
                        node.propose(bytes);
                } else if (key.channel() == readIndexChan) {
                    byte[] bytes = key.data(byte[].class);
                    if (bytes == null)
                        readIndexChan = null;
                    else
                        node.readIndex(bytes);
                } else
                    throw new RuntimeException();
            }
        });
        thread.setName("RaftNodeProposeThread");
        thread.start();

        while (true) {
            SelectionKey<?> key = Selector.open()
                    .register(tickChan, read())
                    .register(node.ready(), read())
                    .select();
            if (key.channel() == tickChan)
                node.tick();
            else if (key.channel() == node.ready()) {
                Ready rd = key.data(Ready.class);
                storage.append(rd.getEntries());
                if (Utils.notEmpty(rd.getMessages())) {
                    Server.sendMessageToPeer(rd.getMessages());
                }
                publishEntries(entriesToApply(rd.getCommittedEntries()));
                publishReadStates(rd.getReadStates());
                node.advance();
            } else
                throw new RuntimeException();
        }
    }

    private void publishReadStates(List<ReadState> readStates) {
        if (readStates == null)
            return;
        for (ReadState readState : readStates) {
            readStateChan.write(readState);
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
/*                case ConfChange:
                    ConfChange confChange = ConfChange.unmarshal(entry.getData());
                    node.applyConfChange(confChange);
                    break;*/
                case Normal:
                    if (entry.getData() == null)
                        break;
                    commitChan.write(entry.getData());
            }
            appliedIndex = entry.getIndex();
        }
    }

    @Override
    public void process(Message message) {
        node.step(message);
    }
}