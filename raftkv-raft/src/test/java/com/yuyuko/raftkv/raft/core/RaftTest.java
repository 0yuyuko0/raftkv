package com.yuyuko.raftkv.raft.core;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.node.Node;
import com.yuyuko.raftkv.raft.storage.*;
import com.yuyuko.raftkv.raft.storage.*;
import com.yuyuko.raftkv.raft.utils.Tuple;
import com.yuyuko.raftkv.raft.utils.Utils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.yuyuko.raftkv.raft.core.Raft.UNLIMIT;
import static com.yuyuko.raftkv.raft.storage.MemoryStorage.newMemoryStorage;
import static com.yuyuko.raftkv.raft.storage.RaftLog.newRaftLog;
import static org.junit.jupiter.api.Assertions.*;

public class RaftTest {
    public interface StateMachine {
        void step(Message m);

        List<Message> readMessages();

        Object getTarget();
    }

    public static class Network {
        Map<Long, StateMachine> peers;
        Map<Long, MemoryStorage> storages;
        Map<Tuple<Long, Long>, Double> dropMsg = new HashMap<>();
        EnumMap<Message.MessageType, Boolean> ignoreMsg = new EnumMap<>(Message.MessageType.class);

        public Network(Map<Long, StateMachine> peers, Map<Long, MemoryStorage> storages) {
            this.peers = peers;
            this.storages = storages;
        }

        public void cut(long one, long other) {
            drop(one, other, 1);
            drop(other, one, 1);
        }

        public void recover() {
            dropMsg = new HashMap<>();
            ignoreMsg = new EnumMap<>(Message.MessageType.class);
        }

        public void ignore(Message.MessageType type) {
            ignoreMsg.put(type, true);
        }

        private void drop(long from, long to, double perc) {
            dropMsg.put(new Tuple<>(from, to), perc);
        }

        public void send(List<Message> messages) {
            messages = new ArrayList<>(messages);
            while (messages.size() != 0) {
                Message m = messages.get(0);
                StateMachine peer = peers.get(m.getTo());
                peer.step(m);
                messages = new ArrayList<>(messages.subList(1, messages.size()));
                messages.addAll(filter(peer.readMessages()));
            }
        }

        private List<Message> filter(List<Message> messages) {
            if (Utils.isEmpty(messages))
                return List.of();
            return messages.stream().filter(m -> {
                if (ignoreMsg.getOrDefault(m.getType(), false))
                    return false;
                switch (m.getType()) {
                    case MsgHup:
                        // hups never go over the network, so don't drop them but panic
                        throw new RaftException("unexpected msgHup");
                    default:
                        Double val = dropMsg.get(new Tuple<>(m.getFrom(),
                                m.getTo()));
                        if (val != null)
                            return ThreadLocalRandom.current().nextDouble(1) >= val;
                        return true;
                }
            }).collect(Collectors.toList());
        }

        public void isolate(long id) {
            for (Long nid : peers.keySet()) {
                if (id != (long) nid)
                    cut(id, nid);
            }
        }
    }

    public static Network newNetwork(StateMachine... peers) {
        int size = peers.length;
        List<Long> peerAddrs = idsBySize(size);
        Map<Long, MemoryStorage> nStorage = new HashMap<>();
        Map<Long, StateMachine> nPeers = new HashMap<>();
        for (int i = 0; i < peers.length; i++) {
            StateMachine peer = peers[i];
            long id = peerAddrs.get(i);
            if (peer == null) {
                nStorage.put(id, newMemoryStorage());
                Raft raft = newTestRaft(id, peerAddrs, 10, 1,
                        nStorage.get(id));
                nPeers.put(id, raftProxy(raft));
            } else if (peer.getTarget() != null) {
                Raft r = (Raft) peer.getTarget();
                r.setId(id);
                Map<Long, Progress> progresses = new HashMap<>();
                peerAddrs.forEach(p -> progresses.put(p, new Progress()));
                r.setProgresses(progresses);
                r.reset(r.getTerm());
                nPeers.put(id, raftProxy(r));
            } else {
                nPeers.put(id, blackHole());
            }
        }
        return new Network(nPeers, nStorage);
    }

    public static StateMachine raftProxy(final Raft target) {
        return ((StateMachine) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class<?>[]{StateMachine.class}, new InvocationHandler() {
                    final Raft raft = target;

                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if (method.getName().equals("step")) {
                            raft.step(((Message) args[0]));
                            return null;
                        } else if (method.getName().equals("readMessages")) {
                            List<Message> messages = raft.getMessages();
                            raft.setMessages(new ArrayList<>());
                            return messages;
                        } else if (method.getName().equals("getTarget")) {
                            return raft;
                        }
                        return null;
                    }
                }));
    }

    static StateMachine blackHole() {
        return ((StateMachine) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class<?>[]{StateMachine.class}, (proxy, method, args) -> null));
    }

    static List<Long> idsBySize(int size) {
        return LongStream.rangeClosed(1, size).boxed().collect(Collectors.toList());
    }

    void setRandomizedElectionTimeout(Raft raft, int r) {
        raft.setRandomizedElectionTimeout(r);
    }

    public static Config newTestConfig(long id, List<Long> peers, int election, int heartbeat,
                           Storage storage) {
        Config config = new Config();
        config.setId(id);
        config.setPeers(peers);
        config.setElectionTick(election);
        config.setHeartbeatTick(heartbeat);
        config.setStorage(storage);
        config.setMaxSizePerMsg(UNLIMIT);
        return config;
    }

    public static Raft newTestRaft(long id, List<Long> peers, int election, int heartbeat,
                             Storage storage) {
        return Raft.newRaft(newTestConfig(id, peers, election, heartbeat, storage));
    }

    StateMachine raftProxyWithEntries(long... terms) {
        MemoryStorage storage = newMemoryStorage();
        for (int i = 0; i < terms.length; i++) {
            storage.append(List.of(new Entry(i + 1, terms[i])));
        }
        Raft testRaft = newTestRaft(1, null, 5, 1, storage);
        testRaft.reset(terms[terms.length - 1]);
        return raftProxy(testRaft);
    }

    // votedWithConfig creates a raft state machine with Vote and Term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not received any logs).
    StateMachine raftProxyWithVoteConfig(long vote, long term) {
        MemoryStorage storage = newMemoryStorage();
        storage.setHardState(new HardState(term, vote));
        Raft raft = newTestRaft(1, null, 5, 1, storage);
        raft.reset(term);
        return raftProxy(raft);
    }

    @Test
    void leaderCycleElection() {
        Network network = newNetwork(null, null, null);
        for (int compaignerId = 1; compaignerId <= 3; compaignerId++) {
            network.send(
                    List.of(Message.builder().from(compaignerId).to(compaignerId).type(Message.MessageType.MsgHup).build())
            );
            int i = 0;
            for (StateMachine value : network.peers.values()) {
                Raft raft = (Raft) value.getTarget();
                if (raft.getId() == compaignerId)
                    assertEquals(Node.NodeState.Leader, raft.getState(), i + "");
                else {
                    assertEquals(Node.NodeState.Follower, raft.getState(), i + "");
                }
                ++i;
            }
        }
    }

    @Test
    void leaderElectionOverwriteNewerLogs() {
        // This network represents the results of the following sequence of
        // events:
        // - Node 1 won the election in term 1.
        // - Node 1 replicated a log entry to node 2 but died before sending
        //   it to other nodes.
        // - Node 3 won the second election in term 2.
        // - Node 3 wrote an entry to its logs but died without sending it
        //   to any other nodes.
        //
        // At this point, nodes 1, 2, and 3 all have uncommitted entries in
        // their logs and could win an election at term 3. The winner's log
        // entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
        // the case where older log entries are overwritten, so this test
        // focuses on the case where the newer entries are lost).
        Network network = newNetwork(
                raftProxyWithEntries(1), // Won first election
                raftProxyWithEntries(1), //Got logs from node 1
                raftProxyWithEntries(2), //Won second election
                raftProxyWithVoteConfig(3, 2),// Node 4: Voted but didn't get logs
                raftProxyWithVoteConfig(3, 2)// Node 5: Voted but didn't get logs
        );

        // Node 1 campaigns. The election fails because a quorum of nodes
        // know about the election that already happened at term 2. Node 1's
        // term is pushed ahead to 2.
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        Raft raft = (Raft) network.peers.get(1L).getTarget();
        assertEquals(Node.NodeState.Follower, raft.getState());
        assertEquals(2, raft.getTerm());
        // Node 1 campaigns again with a higher term. This time it succeeds.
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Leader, raft.getState());
        assertEquals(3, raft.getTerm());
        // Now all nodes agree on a log entry with term 1 at index 1 (and
        // term 3 at index 2).
        network.peers.forEach((id, s) -> {
            Raft target = (Raft) s.getTarget();
            List<Entry> entries = target.getRaftLog().allEntries();
            assertEquals(2, entries.size());
            assertEquals(1, entries.get(0).getTerm());
            assertEquals(3, entries.get(1).getTerm());
        });
    }

    @Test
    void leaderStepdownWhenQuorumActive() {
        Raft raft = newTestRaft(1, List.of(1L, 2L, 3L), 5, 1, newMemoryStorage());
        raft.setCheckQuorum(true);
        raft.becomeCandidate();
        raft.becomeLeader();
        for (int i = 0; i < raft.getElectionTimeout() + 1; i++) {
            raft.step(
                    Message.builder().from(2).type(Message.MessageType.MsgHeartbeatResp).term(raft.getTerm()).build()
            );
            raft.tick();
        }
        assertEquals(Node.NodeState.Leader, raft.getState());
    }

    @Test
    void leaderStepdownWhenQuorumLost() {
        Raft raft = newTestRaft(1, List.of(1L, 2L, 3L), 5, 1, newMemoryStorage());
        raft.setCheckQuorum(true);
        raft.becomeCandidate();
        raft.becomeLeader();
        for (int i = 0; i < raft.getElectionTimeout() + 1; i++) {
            raft.tick();
        }
        assertEquals(Node.NodeState.Follower, raft.getState());
    }

    @Test
    void leaderSupersedingWithCheckQuorum() {
        Raft a = newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft b = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft c = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        a.setCheckQuorum(true);
        b.setCheckQuorum(true);
        c.setCheckQuorum(true);
        Network network = newNetwork(raftProxy(a), raftProxy(b), raftProxy(c));
        b.setRandomizedElectionTimeout(b.getElectionTimeout() + 1);
        for (int i = 0; i < b.getElectionTimeout(); i++) {
            b.tick();
        }
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Leader, a.getState());
        assertEquals(Node.NodeState.Follower, c.getState());
        // 此时b节点还没有选举超时，所以拒绝了C的投票
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Candidate, c.getState());
        for (int i = 0; i < b.getElectionTimeout(); i++) {
            b.tick();
        }
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Leader, c.getState());
    }

    @Test
    void leaderElectionWithCheckQuorum() {
        Raft a = newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft b = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft c = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        a.setCheckQuorum(true);
        b.setCheckQuorum(true);
        c.setCheckQuorum(true);
        Network network = newNetwork(raftProxy(a), raftProxy(b), raftProxy(c));
        b.setRandomizedElectionTimeout(b.getElectionTimeout() + 2);
        a.setRandomizedElectionTimeout(a.getElectionTimeout() + 1);
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Leader, a.getState());
        assertEquals(Node.NodeState.Follower, b.getState());

        b.setRandomizedElectionTimeout(b.getElectionTimeout() + 2);
        a.setRandomizedElectionTimeout(a.getElectionTimeout() + 1);
        for (int i = 0; i < b.getElectionTimeout(); i++) {
            b.tick();
        }

        for (int i = 0; i < a.getElectionTimeout(); i++) {
            a.tick();
        }

        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Leader, c.getState());
        assertEquals(Node.NodeState.Follower, a.getState());

    }

    @Test
    void freeStuckCandidateWithCheckQuorum() {
        Raft a = newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft b = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft c = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        a.setCheckQuorum(true);
        b.setCheckQuorum(true);
        c.setCheckQuorum(true);
        Network network = newNetwork(raftProxy(a), raftProxy(b), raftProxy(c));
        b.setRandomizedElectionTimeout(b.getElectionTimeout() + 1);
        for (int i = 0; i < b.getElectionTimeout(); i++) {
            b.tick();
        }
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        network.isolate(1);
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Candidate, c.getState());
        assertEquals(Node.NodeState.Follower, b.getState());
        assertEquals(b.getTerm() + 1, c.getTerm());
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Candidate, c.getState());
        assertEquals(Node.NodeState.Follower, b.getState());
        assertEquals(b.getTerm() + 2, c.getTerm());
        network.recover();
        network.send(
                List.of(Message.builder().from(1).to(3).type(Message.MessageType.MsgHeartbeat).build())
        );
        assertEquals(Node.NodeState.Follower, a.getState());
        assertEquals(a.getTerm(), c.getTerm());
    }

    // TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
    @Test
    void cannotCommitWithoutNewTermEntry() {
        Network network = newNetwork(null, null, null, null, null);
        //lastIndex 1
        network.send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build()));
        network.cut(1, 3);
        network.cut(1, 4);
        network.cut(1, 5);
        byte[] data = "some data".getBytes();
        //lastIndex 2
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(data))).build()));
        //lastIndex 3
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(data))).build()));

        // 由于中断了与其他三个节点的通信，因此前面提交的值不能被commit
        Raft a = (Raft) network.peers.get(1L).getTarget();
        assertEquals(1, a.getRaftLog().getCommitted());

        //网络恢复
        network.recover();

        network.ignore(Message.MessageType.MsgApp);

        //lastIndex 4
        network.send(
                List.of(Message.builder().from(2).to(2).type(Message.MessageType.MsgHup).build()));

        Raft b = (Raft) network.peers.get(2L).getTarget();
        assertEquals(1, b.getRaftLog().getCommitted());

        network.recover();

        network.send(
                List.of(Message.builder().from(2).to(2).type(Message.MessageType.MsgBeat).build()));
        //lastIndex 5
        network.send(
                List.of(Message.builder().from(2).to(2).type(Message.MessageType.MsgProp).entries(List.of(new Entry(data))).build()));
        assertEquals(5, b.getRaftLog().getCommitted());
    }

    @Test
    void commitWithoutNewTermEntry() {
        Network network = newNetwork(null, null, null, null, null);
        //lastIndex 1
        network.send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build()));
        network.cut(1, 3);
        network.cut(1, 4);
        network.cut(1, 5);
        byte[] data = "some data".getBytes();
        //lastIndex 2
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(data))).build()));
        //lastIndex 3
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(data))).build()));

        // 由于中断了与其他三个节点的通信，因此前面提交的值不能被commit
        Raft a = (Raft) network.peers.get(1L).getTarget();
        assertEquals(1, a.getRaftLog().getCommitted());

        //网络恢复
        network.recover();

        //lastIndex 4
        network.send(
                List.of(Message.builder().from(2).to(2).type(Message.MessageType.MsgHup).build()));

        assertEquals(4, a.getRaftLog().getCommitted());
    }

    @Test
    void recvMsgUnreachable() {
        List<Entry> entries = List.of(new Entry(1, 1), new Entry(2, 1), new Entry(3, 1));
        MemoryStorage storage = newMemoryStorage();
        storage.append(entries);
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, storage);
        raft.becomeCandidate();
        raft.becomeLeader();
        raft.setMessages(new ArrayList<>());
        Progress progress = raft.getProgresses().get(2L);
        progress.setMatch(3);
        progress.becomeReplicate();
        progress.optimisticUpdate(5);
        raft.step(Message.builder()
                .from(2)
                .to(1)
                .type(Message.MessageType.MsgUnreachable)
                .build());
        assertEquals(Progress.ProgressState.Probe, progress.getState());
        assertEquals(progress.getMatch() + 1, progress.getNext());
    }

    @Test
    void broadcastHeartbeat() {
        long offset = 1000;
        Snapshot snapshot = new Snapshot(
                null,
                new SnapshotMetadata(
                        new ConfState(List.of(1L, 2L, 3L)),
                        1000,
                        1
                )
        );
        MemoryStorage storage = newMemoryStorage();
        storage.applySnapshot(snapshot);
        Raft raft = newTestRaft(1, null, 10, 1, storage);
        raft.setTerm(1L);
        raft.becomeCandidate();
        raft.becomeLeader();
        for (int i = 0; i < 10; i++) {
            raft.appendEntries(
                    List.of(new Entry(i + 1)));
        }
        Progress _2 = raft.getProgresses().get(2L);
        Progress _3 = raft.getProgresses().get(3L);
        _2.setMatch(5L);
        _2.setNext(6L);
        _3.setMatch(raft.getRaftLog().lastIndex());
        _3.setNext(raft.getRaftLog().lastIndex() + 1);
        raft.step(Message.builder().type(Message.MessageType.MsgBeat).build());
        List<Message> messages = raft.getMessages();
        raft.setMessages(new ArrayList<>());
        Map<Long, Long> wantCommitMap = new HashMap<>(Map.of(
                2L, Math.min(raft.getRaftLog().getCommitted(), _2.getMatch()),
                3L, Math.min(raft.getRaftLog().getCommitted(), _3.getMatch())
        ));
        for (int i = 0; i < messages.size(); i++) {
            Message m = messages.get(i);
            Assertions.assertEquals(Message.MessageType.MsgHeartbeat, m.getType());
            assertEquals(0, m.getIndex());
            assertEquals(0, m.getLogTerm());
            assertNotEquals(0, wantCommitMap.get(m.getTo()));
            assertEquals(m.getCommit(), wantCommitMap.get(m.getTo()));
            wantCommitMap.remove(m.getTo());
            assertTrue(Utils.isEmpty(m.getEntries()));
        }
    }

    @Test
    void leaderElection() {
        class Test {
            Network network;
            Node.NodeState state;
            long expTerm;

            public Test(Network network, Node.NodeState state, long expTerm) {
                this.network = network;
                this.state = state;
                this.expTerm = expTerm;
            }
        }

        Test[] tests = new Test[]{
                new Test(newNetwork(null, null, null), Node.NodeState.Leader, 1),
                new Test(newNetwork(null, null, blackHole()), Node.NodeState.Leader, 1),
                new Test(newNetwork(null, blackHole(), blackHole()), Node.NodeState.Candidate, 1),
                new Test(newNetwork(null, blackHole(), null, blackHole()),
                        Node.NodeState.Candidate, 1),
                new Test(newNetwork(null, blackHole(), null, null, blackHole()),
                        Node.NodeState.Leader, 1),
                // three logs further along than 0, but in the same term so rejections
                // are returned instead of the votes being ignored.
                new Test(newNetwork(null, raftProxyWithEntries(1), raftProxyWithEntries(1),
                        raftProxyWithEntries(1, 1), null), Node.NodeState.Follower, 1)
        };
        for (int i = 0; i < tests.length; i++) {
            Test test = tests[i];
            test.network.send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build()));
            Raft target = (Raft) test.network.peers.get(1L).getTarget();
            int finalI = i;
            assertAll(
                    () -> assertEquals(test.expTerm, target.getTerm(), finalI + ""),
                    () -> assertEquals(test.state, target.getState(), finalI + "")
            );
        }
    }

    @Test
    void campaignWhileLeader() {
        Raft raft = Raft.newRaft(newTestConfig(1, List.of(1L), 5, 1, newMemoryStorage()));
        assertEquals(Node.NodeState.Follower, raft.getState());
        raft.step(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build());
        assertEquals(Node.NodeState.Leader, raft.getState());
        raft.step(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build());
        assertEquals(Node.NodeState.Leader, raft.getState());
    }

    @Test
    void restoreIgnoreSnapshot() {
        List<Entry> entries = List.of(
                new Entry(1, 1),
                new Entry(2, 1),
                new Entry(3, 1)
        );
        long commit = 1;
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.getRaftLog().append(entries);
        raft.getRaftLog().commitTo(commit);
        Snapshot snapshot = new Snapshot(
                null,
                new SnapshotMetadata(
                        new ConfState(List.of(1L, 2L)),
                        commit,
                        1
                )
        );
        assertFalse(raft.restore(snapshot));
        assertEquals(commit, raft.getRaftLog().getCommitted());

        snapshot.getMetadata().setIndex(commit + 1);
        assertFalse(raft.restore(snapshot));
        assertEquals(commit + 1, raft.getRaftLog().getCommitted());
    }

    @Test
    void restoreFromSnapMsg() {
        Raft raft = newTestRaft(2, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.step(
                Message.builder().from(1L).to(2L).type(Message.MessageType.MsgSnap).snapshot(testingSnap).build()
        );
        assertEquals(1, raft.getLead());

        Raft raft2 = newTestRaft(2, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft2.becomeCandidate();

        raft2.step(
                Message.builder().from(1L).to(2L).type(Message.MessageType.MsgSnap).snapshot(testingSnap).build()
        );
        assertEquals(1, raft2.getLead());
    }

    @Test
    void restoreFromAppMsg() {
        Raft raft = newTestRaft(2, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.becomeCandidate();
        raft.step(
                Message.builder().from(1L).to(2L).type(Message.MessageType.MsgApp).index(1L).logTerm(1L).entries(List.of(new Entry(1L, 1L))).commit(0L).build()
        );
        assertEquals(1L, raft.getLead());
    }

    @Test
    void providerSnap() {
        Snapshot snapshot = new Snapshot(
                null,
                new SnapshotMetadata(
                        new ConfState(List.of(1L, 2L)),
                        11,
                        11
                )
        );
        MemoryStorage storage = newMemoryStorage();
        Raft raft = newTestRaft(1, List.of(1L), 10, 1, storage);
        raft.restore(snapshot);
        raft.becomeCandidate();
        raft.becomeLeader();
        Progress progress = raft.getProgresses().get(2L);
        progress.setNext(raft.getRaftLog().firstIndex());
        raft.step(
                Message.builder()
                        .from(2L)
                        .to(1L)
                        .type(Message.MessageType.MsgAppResp)
                        .index(progress.getNext() - 1)
                        .reject(true)
                        .build()
        );
        List<Message> messages = raft.getMessages();
        assertEquals(1, messages.size());
        Assertions.assertEquals(Message.MessageType.MsgSnap, messages.get(0).getType());
    }

    @Test
    void singleNodeCommit() {
        Network network = newNetwork((StateMachine) null);
        network.send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build()));
        network.send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(
                "some".getBytes()))).build()));
        network.send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(
                "some".getBytes()))).build()));
        Raft target = (Raft) network.peers.get(1L).getTarget();
        assertEquals(3, target.getRaftLog().getCommitted());
    }

    @Test
    void duelingCandidates() {
        Raft a = newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft b = newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft c = newTestRaft(3, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Network network = newNetwork(raftProxy(a), raftProxy(b), raftProxy(c));
        network.cut(1, 3);
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );
        // 1 becomes leader since it receives votes from 1 and 2
        Raft raft;
        raft = (Raft) network.peers.get(1L).getTarget();
        assertEquals(Node.NodeState.Leader, raft.getState());
        raft = (Raft) network.peers.get(3L).getTarget();
        assertEquals(Node.NodeState.Candidate, raft.getState());

        network.recover();

        // candidate 3 now increases its term and tries to vote again
        // we expect it to disrupt the leader 1 since it has a higher term
        // 3 will be follower again since both 1 and 2 rejects its vote request
        // since 3 does not have a long enough log
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );

        RaftLog wantLog = newRaftLog(
                new MemoryStorage(List.of(new Entry(), new Entry(1, 1))),
                new Unstable(2),
                1
        );
        assertAll(
                () -> assertEquals(Node.NodeState.Follower, a.getState()),
                () -> assertEquals(2, a.getTerm()),
                () -> assertEquals(itoa(wantLog), itoa(a.getRaftLog()))
        );
        assertAll(
                () -> assertEquals(Node.NodeState.Follower, b.getState()),
                () -> assertEquals(2, b.getTerm()),
                () -> assertEquals(itoa(wantLog), itoa(b.getRaftLog()))
        );
        assertAll(
                () -> assertEquals(Node.NodeState.Follower, c.getState()),
                () -> assertEquals(2, c.getTerm()),
                () -> assertEquals(itoa(newRaftLog(newMemoryStorage())), itoa(c.getRaftLog()))
        );
    }

    @Test
    void proposalViaProxy() {
        Network[] networks = new Network[]{
                newNetwork(null, null, null),
                newNetwork(null, null, blackHole())
        };
        for (int i = 0; i < networks.length; i++) {
            Network network = networks[i];
            //promote 0 the leader
            network.send(
                    List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build()));

            byte[] data = "somedata".getBytes();
            network.send(
                    List.of(Message.builder().from(2).to(2).type(Message.MessageType.MsgProp).entries(List.of(new Entry(
                            data))).build()));
            RaftLog wantLog = newRaftLog(
                    new MemoryStorage(List.of(new Entry(), new Entry(1, 1), new Entry(2, 1,
                            data))),
                    new Unstable(3),
                    2);
            String base = itoa(wantLog);
            int finalI = i;
            network.peers.forEach((id, p) -> {
                Raft target = (Raft) p.getTarget();
                if (target != null) {
                    String l = itoa(target.getRaftLog());
                    assertEquals(base, l, finalI + "");
                } else {
                    System.out.println(String.format("#%d: empty log", id));
                }
            });
            StateMachine stateMachine = network.peers.get(1L);
            Raft target = (Raft) stateMachine.getTarget();
            Assertions.assertEquals(1, target == null ? 0 : target.getTerm(), String.format("#%d:term=%d," +
                    "want=%d", i, target.getTerm(), 1));

        }
    }

    @Test
    void proposal() {
        class Test {
            Network network;
            boolean success;

            public Test(Network network, boolean success) {
                this.network = network;
                this.success = success;
            }
        }
        Test[] tests = new Test[]{
                new Test(newNetwork(null, null, null), true),
                new Test(newNetwork(null, null, blackHole()), true),
                new Test(newNetwork(null, blackHole(), blackHole()), false),
                new Test(newNetwork(null, blackHole(), blackHole(), null), false),
                new Test(newNetwork(null, blackHole(), blackHole(), null, null), true),
        };

        for (int i = 0; i < tests.length; i++) {
            int finalI = i;

            Test test = tests[i];
            byte[] data = "somedata".getBytes();
            // promote 0 the leader
            test.network.send(List.of(
                    Message.builder().from(1).to(1).type(Message.MessageType.MsgHup)
                            .build()));
            test.network.
                    send(List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(data)))
                            .build())
                    );
            RaftLog wantLog = newRaftLog(newMemoryStorage());
            if (test.success) {
                wantLog = newRaftLog(
                        new MemoryStorage(List.of(new Entry(), new Entry(1, 1), new Entry(2, 1,
                                data))),
                        new Unstable(3),
                        2);
            }
            String base = itoa(wantLog);
            test.network.peers.forEach((id, p) -> {
                Raft target = (Raft) p.getTarget();
                if (target != null) {
                    String l = itoa(target.getRaftLog());
                    assertEquals(base, l, finalI + "");
                } else {
                    System.out.println(String.format("#%d: empty log", id));
                }
            });
            StateMachine stateMachine = test.network.peers.get(1L);
            Raft target = (Raft) stateMachine.getTarget();

            Assertions.assertEquals(1, target == null ? 0 : target.getTerm(), String.format("#%d:term=%d," +
                    "want=%d", i, target.getTerm(), 1));
        }
    }

    Snapshot testingSnap = new Snapshot(
            null,
            new SnapshotMetadata(
                    new ConfState(List.of(1L, 2L)),
                    11,
                    11
            )
    );

    @Test
    void pendingSnapshotPauseReplication() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.restore(testingSnap);
        raft.becomeCandidate();
        raft.becomeLeader();
        raft.getProgresses().get(2L).becomeSnapshot(11L);
        assertThrows(Throwable.class, () -> raft.step(
                Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry("somedata".getBytes()))).build()
        ));

    }

    @Test
    void snapshotFailure() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.restore(testingSnap);
        raft.becomeCandidate();
        raft.becomeLeader();
        Progress _2 = raft.getProgresses().get(2L);
        _2.becomeSnapshot(11L);
        _2.setNext(1L);
        raft.step(
                Message.builder().from(2).to(1).type(Message.MessageType.MsgSnapStatus).reject(true).build()
        );
        assertEquals(0L, _2.getPendingSnapshot());
        assertEquals(1L, _2.getNext());
        assertTrue(_2.isPaused());
    }

    @Test
    void snapshotSucceed() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.restore(testingSnap);
        raft.becomeCandidate();
        raft.becomeLeader();
        Progress _2 = raft.getProgresses().get(2L);
        _2.becomeSnapshot(11L);
        _2.setNext(1L);
        raft.step(
                Message.builder().from(2).to(1).type(Message.MessageType.MsgSnapStatus).reject(false).build()
        );
        assertEquals(0L, _2.getPendingSnapshot());
        assertEquals(12L, _2.getNext());
        assertTrue(_2.isPaused());
    }

    @Test
    void snapshotAbort() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.restore(testingSnap);
        raft.becomeCandidate();
        raft.becomeLeader();
        Progress _2 = raft.getProgresses().get(2L);
        _2.becomeSnapshot(11L);
        _2.setNext(1L);

        // A successful msgAppResp that has a higher/equal index than the
        // pending snapshot should abort the pending snapshot.
        raft.step(
                Message.builder().from(2).to(1).type(Message.MessageType.MsgAppResp).index(11).build()
        );
        assertEquals(0L, _2.getPendingSnapshot());
        assertEquals(12L, _2.getNext());
    }

    @Test
    void sendingSnapshotSetPendingSnapshot() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.restore(testingSnap);
        raft.becomeCandidate();
        raft.becomeLeader();
        raft.getProgresses().get(2L).setNext(raft.getRaftLog().firstIndex());
        raft.step(
                Message.builder().from(2).to(1).type(Message.MessageType.MsgAppResp)
                        .index(raft.getProgresses().get(2L).getNext() - 1).reject(true).build()
        );
        assertEquals(11L, raft.getProgresses().get(2L).getPendingSnapshot());
    }

    @Test
    void raftFreesReadOnlyMem() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 5, 1, newMemoryStorage());
        raft.becomeCandidate();
        raft.becomeLeader();
        raft.getRaftLog().commitTo(raft.getRaftLog().lastIndex());
        byte[] ctx = "ctx".getBytes();
        raft.step(
                Message.builder().from(2).type(Message.MessageType.MsgReadIndex).entries(List.of(new Entry(ctx))).build()
        );
        List<Message> messages = raft.getMessages();
        assertEquals(1, messages.size());
        Assertions.assertEquals(Message.MessageType.MsgHeartbeat, messages.get(0).getType());
        assertArrayEquals(ctx, messages.get(0).getContext());
        assertEquals(1, raft.getReadOnly().getReadIndexQueue().size());
        assertEquals(1, raft.getReadOnly().getPendingReadIndex().size());
        assertTrue(raft.getReadOnly().getPendingReadIndex().containsKey(new String(ctx)));
        raft.step(
                Message.builder().from(2).type(Message.MessageType.MsgHeartbeatResp).context(ctx).build()
        );
        assertEquals(0, raft.getReadOnly().getReadIndexQueue().size());
        assertEquals(0, raft.getReadOnly().getPendingReadIndex().size());
        assertFalse(raft.getReadOnly().getPendingReadIndex().containsKey(new String(ctx)));
    }

    @Test
    void readOnlyForNewLeader() {
        Raft a = newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, new MemoryStorage(
                new HardState(2, 1, Raft.NONE),
                List.of(new Entry(), new Entry(1, 1), new Entry(2, 1))
        ));
        Config bConfig = newTestConfig(2, List.of(1L, 2L, 3L), 10, 1, new MemoryStorage(
                new HardState(2, 1, Raft.NONE),
                List.of(new Entry(), new Entry(1, 1), new Entry(2, 1))
        ));
        bConfig.setApplied(2);
        Raft b = Raft.newRaft(bConfig);
        Config cConfig = newTestConfig(3, List.of(1L, 2L, 3L), 10, 1, new MemoryStorage(
                new HardState(2, 1, Raft.NONE),
                List.of(new Entry(), new Entry(1, 1), new Entry(2, 1))
        ));
        cConfig.setApplied(2);
        Raft c = Raft.newRaft(cConfig);
        Network network = newNetwork(raftProxy(a), raftProxy(b), raftProxy(c));
        network.ignore(Message.MessageType.MsgApp);
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        assertEquals(Node.NodeState.Leader, a.getState());
        byte[] wCtx = "ctx".getBytes();
        long wIdx = 4;
        network.send(
                List.of(Message.builder().from(1L).to(1L).type(Message.MessageType.MsgReadIndex).entries(List.of(new Entry(wCtx))).build())
        );
        assertEquals(0, a.getReadStates().size());
        network.recover();
        // Force peer a to commit a log entry at its term
        for (int i = 0; i < a.getHeartbeatTimeout(); i++) {
            a.tick();
        }
        network.send(
                List.of(
                        Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry())).build()
                )
        );
        assertEquals(4, a.getRaftLog().getCommitted());
        assertEquals(a.getTerm(),
                a.getRaftLog().zeroTermOnErrCompacted(a.getRaftLog().getCommitted()));
        network.send(
                List.of(
                        Message.builder().from(1).to(1).type(Message.MessageType.MsgReadIndex).entries(List.of(new Entry(wCtx))).build()
                )
        );
        assertEquals(1, a.getReadStates().size());
        assertEquals(wIdx, a.getReadStates().get(0).getIndex());
        assertArrayEquals(wCtx, a.getReadStates().get(0).getRequestCtx());
    }

    @Test
    void readOnlyForFollower() {
        Raft a = newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, new MemoryStorage(
                new HardState(2, 1, Raft.NONE),
                List.of(new Entry(), new Entry(1, 1), new Entry(2, 1))
        ));
        Config bConfig = newTestConfig(2, List.of(1L, 2L, 3L), 10, 1, new MemoryStorage(
                new HardState(2, 1, Raft.NONE),
                List.of(new Entry(), new Entry(1, 1), new Entry(2, 1))
        ));
        bConfig.setApplied(2);
        Raft b = Raft.newRaft(bConfig);
        Config cConfig = newTestConfig(3, List.of(1L, 2L, 3L), 10, 1, new MemoryStorage(
                new HardState(2, 1, Raft.NONE),
                List.of(new Entry(), new Entry(1, 1), new Entry(2, 1))
        ));
        cConfig.setApplied(2);
        Raft c = Raft.newRaft(cConfig);
        Network network = newNetwork(raftProxy(a), raftProxy(b), raftProxy(c));
        network.send(
                List.of(Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build())
        );
        byte[] wCtx = "ctx".getBytes();
        long wIdx = 3;
        //test follower redirect
        network.send(
                List.of(
                        Message.builder().from(2).to(2).type(Message.MessageType.MsgReadIndex).entries(List.of(new Entry(wCtx))).build()
                )
        );
        assertEquals(1, b.getReadStates().size());
        assertEquals(wIdx, b.getReadStates().get(0).getIndex());
        assertArrayEquals(wCtx, b.getReadStates().get(0).getRequestCtx());
    }

/*    @Disabled
    void stepConfig() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.becomeCandidate();
        raft.becomeLeader();
        long l = raft.getRaftLog().lastIndex();
        raft.step(
                Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(Entry.EntryType.ConfChange))).build()
        );
        assertEquals(raft.getRaftLog().lastIndex(), l + 1);
        assertTrue(raft.isPendingConf());
    }*/

/*    @Disabled
    void stepIgnoreConfig() {
        Raft raft = newTestRaft(1, List.of(1L, 2L), 10, 1, newMemoryStorage());
        raft.becomeCandidate();
        raft.becomeLeader();
        raft.step(
                Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(Entry.EntryType.ConfChange))).build()
        );
        long l = raft.getRaftLog().lastIndex();
        boolean pendingConf = raft.isPendingConf();
        raft.step(
                Message.builder().from(1).to(1).type(Message.MessageType.MsgProp).entries(List.of(new Entry(Entry.EntryType.ConfChange))).build()
        );
        List<Entry> wEnts = List.of(new Entry(3, 1, Entry.EntryType.Normal, null));
        assertArrayEquals(wEnts.toArray(),
                raft.getRaftLog().entries(((int) (l + 1)), UNLIMIT).toArray());
        assertEquals(raft.isPendingConf(), pendingConf);
    }*/


    private String itoa(RaftLog raftLog) {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("commited:%d\n", raftLog.getCommitted()))
                .append(String.format("applied:%d\n", raftLog.getApplied()));

        List<Entry> allEntries = raftLog.allEntries();
        for (int i = 0; i < allEntries.size(); i++) {
            sb.append(String.format("#%d: %s\n", i, allEntries.get(i)));
        }
        return sb.toString();
    }
}