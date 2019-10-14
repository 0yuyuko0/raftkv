package com.yuyuko.raftkv.raft.node;

import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.core.*;
import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.raft.storage.MemoryStorage;
import com.yuyuko.raftkv.raft.storage.Snapshot;
import com.yuyuko.raftkv.raft.storage.SnapshotMetadata;
import com.yuyuko.selector.Channel;
import com.yuyuko.selector.SelectionKey;
import com.yuyuko.selector.Selector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.yuyuko.raftkv.raft.core.Raft.UNLIMIT;
import static com.yuyuko.raftkv.raft.storage.MemoryStorage.newMemoryStorage;
import static com.yuyuko.selector.SelectionKey.*;
import static org.junit.jupiter.api.Assertions.*;


class NodeTest {

 /*   @Test
    @Disabled
    void startNode() {
        ConfChange confChange = new ConfChange(ConfChange.ConfChangeType.AddNode, 1, null);
        byte[] ccBytes = confChange.marshal();
        Ready ready1 = new Ready();
        ready1.setHardState(new HardState(1, 1, 0));
        ready1.setEntries(List.of(new Entry(1, 1, Entry.EntryType.ConfChange, ccBytes)));
        ready1.setCommittedEntries(List.of(new Entry(1, 1, Entry.EntryType.ConfChange, ccBytes)));
        Ready ready2 = new Ready();
        ready2.setHardState(new HardState(3, 2, 1));
        ready2.setEntries(List.of(new Entry(3, 2, "foo".getBytes())));
        ready2.setCommittedEntries(List.of(new Entry(3, 2, "foo".getBytes())));

        Ready[] wants = new Ready[]{
                ready1,
                ready2
        };
        Config config = new Config();
        config.setId(1);
        config.setElectionTick(10);
        config.setHeartbeatTick(1);
        MemoryStorage storage = newMemoryStorage();
        config.setStorage(storage);
        config.setMaxSizePerMsg(UNLIMIT);

        Node node = DefaultNode.startNode(config, List.of(new Peer(1L, null)));
        Ready g = node.ready().read();
        assertEquals(wants[0], g, "g = wants[0]");
        storage.append(g.getEntries());
        node.advance();

        node.campaign();
        Ready rd = node.ready().read();
        storage.append(rd.getEntries());
        node.advance();
        node.propose("foo".getBytes());
        Ready g2 = node.ready().read();
        assertEquals(wants[1], g2);
        storage.append(g2.getEntries());
        node.advance();

        Channel<Object> timer = new Channel<>();
        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            timer.write(null);
        }).start();

        SelectionKey<?> key = Selector.open()
                .register(node.ready(), read())
                .register(timer, read())
                .select();
        if(key.channel() == node.ready())
            fail("unexpected Ready");
        node.stop();
    }
*/
    @RepeatedTest(100)
    void tick() throws InterruptedException {
        DefaultNode node = DefaultNode.newNode();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, newMemoryStorage());
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            node.run(raft);
        });
        thread.start();
        while (thread.getState() != Thread.State.WAITING)
            Thread.onSpinWait();

        int electionElapsed = raft.getElectionElapsed();
        node.tick();
        TimeUnit.MILLISECONDS.sleep(10);
        node.stop();
        assertEquals(electionElapsed + 1, raft.getElectionElapsed());
    }

    @Test
    void campaign() {
    }

    @RepeatedTest(100)
    /**
     * 无leader的节点会阻塞propose请求直到leader出现
     */
    void blockPropose() throws InterruptedException {
        MemoryStorage storage = newMemoryStorage();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, storage);
        DefaultNode node = DefaultNode.newNode();
        Channel<Throwable> chan = new Channel<>(1);
        new Thread(() -> node.run(raft)).start();
        new Thread(() -> {
            try {
                node.propose("somedata".getBytes());
                chan.write(null);
            } catch (Throwable ex) {
                chan.write(ex);
            }
        }).start();
        TimeUnit.MILLISECONDS.sleep(10);
        node.campaign();

        SelectionKey<?> key = Selector.open()
                .register(chan, read())
                .select();
        assertNull(key.data());
        node.stop();
    }

    @Test
    void propose() {
        List<Message> messages = new ArrayList<>();
        MemoryStorage storage = newMemoryStorage();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, storage);
        DefaultNode node = DefaultNode.newNode();
        new Thread(() -> node.run(raft)).start();
        node.campaign();
        while (true) {
            Ready rd = node.ready().read();
            storage.append(rd.getEntries());
            if (rd.getSoftState().getLead() == raft.getId()) {
                raft.setStepFunc((r, m) -> messages.add(m));
                node.advance();
                break;
            }
            node.advance();
        }
        node.propose("somedata".getBytes());
        node.stop();
        assertEquals(1, messages.size());
        assertEquals(Message.MessageType.MsgProp, messages.get(0).getType());
        assertArrayEquals("somedata".getBytes(), messages.get(0).getEntries().get(0).getData());
    }

    @RepeatedTest(10)
    @Disabled
    void benchmark() throws InterruptedException {
        long start = System.currentTimeMillis();
        benchmarkOneNode(100000);
        long end = System.currentTimeMillis();
        System.err.println(end - start + "ms");
    }

    void benchmarkOneNode(int proposeCnt) throws InterruptedException {
        DefaultNode node = DefaultNode.newNode();
        MemoryStorage storage = newMemoryStorage();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, storage);
        new Thread(() -> node.run(raft)).start();
        node.campaign();
        new Thread(() -> {
            for (int i = 0; i < proposeCnt; i++) {
                node.propose("foo".getBytes());
            }
        }).start();
        while (true) {
            Ready rd = node.ready().read();
            storage.append(rd.getEntries());
            // a reasonable disk sync latency
            TimeUnit.MILLISECONDS.sleep(1);
            node.advance();
            if (rd.getHardState().getCommit() == proposeCnt + 1)
                return;
        }
    }

/*    @Test
    void proposeConfig() {
        List<Message> messages = new ArrayList<>();
        MemoryStorage storage = newMemoryStorage();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, storage);
        DefaultNode node = DefaultNode.newNode();
        new Thread(() -> node.run(raft)).start();
        node.campaign();
        while (true) {
            Ready rd = node.ready().read();
            storage.append(rd.getEntries());
            if (rd.getSoftState().getLead() == raft.getId()) {
                raft.setStepFunc((r, m) -> messages.add(m));
                node.advance();
                break;
            }
            node.advance();
        }
        ConfChange cc = new ConfChange(ConfChange.ConfChangeType.AddNode, 1, null);
        byte[] data = cc.marshal();
        node.proposeConfChange(cc);

        node.stop();
        assertEquals(1, messages.size());
        assertEquals(Message.MessageType.MsgProp, messages.get(0).getType());
        assertArrayEquals(data, messages.get(0).getEntries().get(0).getData());
    }*/

    @Test
    void step() {
        Message.MessageType[] values = Message.MessageType.values();
        for (int i = 0; i < values.length; i++) {
            DefaultNode node = new DefaultNode();
            node.setRecvChan(new Channel<>(1));
            node.setPropChan(new Channel<>(1));
            node.step(
                    Message.builder().type(values[i]).build());
            int finalI = i;

            if (values[i] == Message.MessageType.MsgProp) {
                SelectionKey<?> key = Selector.open()
                        .register(node.getPropChan(), read())
                        .fallback(fallback())
                        .select();
                if (key.type() == FALLBACK)
                    fail(finalI + "");
            } else {
                if (Message.isLocalMsg(values[i])) {
                    SelectionKey<?> key = Selector.open()
                            .register(node.getRecvChan(), read())
                            .fallback(fallback())
                            .select();
                    if (key.type() == READ)
                        fail(i + "");
                } else {
                    SelectionKey<?> key = Selector.open()
                            .register(node.getRecvChan(), read())
                            .fallback(fallback())
                            .select();
                    if (key.type() == FALLBACK)
                        fail(i + "");
                }
            }
        }
    }

    @RepeatedTest(10)
    void advance() {
        MemoryStorage storage = newMemoryStorage();
        Config config = new Config();
        config.setId(1);
        config.setElectionTick(10);
        config.setHeartbeatTick(1);
        config.setStorage(storage);
        config.setMaxSizePerMsg(UNLIMIT);

        Node node = DefaultNode.startNode(config, List.of(new Peer(1L, null)));

        Ready rd = node.ready().read();
        storage.append(rd.getEntries());
        node.advance();

        node.campaign();
        node.ready().read();

        node.propose("foo".getBytes());

        Channel<Object> timer = new Channel<>();
        new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                timer.write(null);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        SelectionKey<?> key = Selector.open()
                .register(node.ready(), read())
                .register(timer, read())
                .select();
        if (key.channel() == node.ready())
            fail();
        storage.append(rd.getEntries());
        node.advance();
        node.ready().read();
    }

    @Test
    void readIndex() {
        List<Message> messages = new ArrayList<>();
        StepFunction appendStep = (raft, m) -> {
            messages.add(m);
        };
        List<ReadState> wReadState = List.of(new ReadState(1, "somedata".getBytes()));
        MemoryStorage storage = newMemoryStorage();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, storage);
        raft.setReadStates(wReadState);
        DefaultNode node = DefaultNode.newNode();
        new Thread(() -> node.run(raft)).start();
        node.campaign();
        while (true) {
            Ready rd = node.ready().read();
            assertArrayEquals(wReadState.toArray(), rd.getReadStates().toArray());
            storage.append(rd.getEntries());
            if (rd.getSoftState().getLead() == raft.getId()) {
                node.advance();
                break;
            }
            node.advance();
        }
        raft.setStepFunc(appendStep);
        byte[] bytes = "somedata2".getBytes();
        node.readIndex(bytes);
        node.stop();
        assertEquals(1, messages.size());
        assertEquals(Message.MessageType.MsgReadIndex, messages.get(0).getType());
        assertArrayEquals(bytes, messages.get(0).getEntries().get(0).getData());
    }

    @Test
    void readIndexToOldLeader() {
        Raft r1 = RaftTest.newTestRaft(1, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft r2 = RaftTest.newTestRaft(2, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());
        Raft r3 = RaftTest.newTestRaft(3, List.of(1L, 2L, 3L), 10, 1, newMemoryStorage());

        RaftTest.Network network = RaftTest.newNetwork(
                RaftTest.raftProxy(r1), RaftTest.raftProxy(r2), RaftTest.raftProxy(r3));

        network.send(
                List.of(
                        Message.builder().from(1).to(1).type(Message.MessageType.MsgHup).build()
                )
        );
        var testEntries = List.of(new Entry("testdata".getBytes()));
        // write readindex request to r2(follower)
        r2.step(
                Message.builder().from(2).to(2).type(Message.MessageType.MsgReadIndex).entries(testEntries).build()
        );
        // verify r2(follower) forwards this message to r1(leader) with term not set
        assertEquals(1, r2.getMessages().size());
        Message readIdxMsg1 =
                Message.builder().from(2).to(1).type(Message.MessageType.MsgReadIndex).entries(testEntries).build();
        assertEquals(readIdxMsg1, r2.getMessages().get(0));

        // write readindex request to r3(follower)
        r3.step(
                Message.builder().from(3).to(3).type(Message.MessageType.MsgReadIndex).entries(testEntries).build()
        );
        assertEquals(1, r3.getMessages().size());
        Message readIdxMsg2 =
                Message.builder().from(3).to(1).type(Message.MessageType.MsgReadIndex).entries(testEntries).build();
        assertEquals(readIdxMsg2, r3.getMessages().get(0));

        // now elect r3 as leader
        network.send(
                List.of(Message.builder().from(3).to(3).type(Message.MessageType.MsgHup).build())
        );

        // let r1 steps the two messages previously we got from r2, r3
        r1.step(readIdxMsg1);
        r1.step(readIdxMsg2);

        // verify r1(follower) forwards these messages again to r3(new leader)
        assertEquals(2, r1.getMessages().size());
        Message readIdxMsg3 =
                Message.builder().from(1).to(3).type(Message.MessageType.MsgReadIndex).entries(testEntries).build();
        assertEquals(readIdxMsg3, r1.getMessages().get(0));
        assertEquals(readIdxMsg3, r1.getMessages().get(1));

    }

    @Test
    void readyContainUpdates() {
        class Test {
            Ready rd;
            boolean wContain;

            public Test(Ready rd, boolean wContain) {
                this.rd = rd;
                this.wContain = wContain;
            }
        }
        Test[] tests = new Test[]{
                new Test(new Ready(), false),
                new Test(new Ready().setSoftState(new SoftState(1, Node.NodeState.Follower)), true),
                new Test(new Ready().setHardState(new HardState(0, 0, 1)), true),
                new Test(new Ready().setHardState(new HardState()), false),
                new Test(new Ready().setCommittedEntries(List.of(new Entry(1, 1))), true),
                new Test(new Ready().setMessages(List.of(Message.builder().build())), true),
                new Test(new Ready().setSnapshot(new Snapshot(null, new SnapshotMetadata(1))),
                        true),
        };
        int cnt = 0;
        for (Test test : tests) {
            cnt++;
            assertEquals(test.wContain, test.rd.containsUpdate(), cnt + "");
        }
    }

    @Test
    void stop() {
        DefaultNode node = DefaultNode.newNode();
        Raft raft = RaftTest.newTestRaft(1, List.of(1L), 10, 1, newMemoryStorage());
        Channel<Object> donec = new Channel<>();
        new Thread(() -> {
            node.run(raft);
            donec.write(1L);
        }).start();
        Status status = node.status();
        node.stop();
        Channel<Object> timer = new Channel<>();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            timer.write(1);
        }).start();
        SelectionKey<?> key = Selector.open()
                .register(donec, read())
                .register(timer, read())
                .select();
        if (key.channel() == timer)
            fail("timed out waiting for node to stop!");
        assertNotEquals(new Status(), status);
        // Further status should return be empty, the node is stopped.
        assertEquals(new Status(), node.status());

        node.stop();
    }
}